/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { inject, Injectable } from '@angular/core';
import { BehaviorSubject, forkJoin, Observable, of, Subscription } from 'rxjs';
import { catchError, map } from 'rxjs/operators';

import { WatermarkSample } from '@flink-runtime-web/interfaces';

import { MetricsService } from './metrics.service';
import { StatusService } from './status.service';

/**
 * Rolling-window (60 min) history of watermark lag per vertex. Lag is
 * computed client-side as `Date.now() - lowWatermark`; an idle vertex
 * (where `lowWatermark` is unset / Long.MIN_VALUE) is recorded with
 * `idle = true` and `lagMs = NaN` so the chart can render a gap rather
 * than infinite lag.
 *
 * Self-polls via `statusService.refresh$` while at least one consumer is
 * tracking. When the last consumer releases, polling halts.
 *
 * Optional localStorage persistence survives browser refresh / tab close
 * for the *same* jobId. Toggled at runtime by the user.
 */
@Injectable({ providedIn: 'root' })
export class WatermarkLagHistoryService {
  private static readonly RETENTION_MS = 60 * 60 * 1000;
  private static readonly PERSIST_FLAG_KEY = 'flink:watermark-lag:persist';
  private static readonly PERSIST_DATA_KEY_PREFIX = 'flink:watermark-lag:samples:';
  private static readonly PERSIST_WRITE_DEBOUNCE_MS = 2_000;

  private readonly metricsService = inject(MetricsService);
  private readonly statusService = inject(StatusService);

  private currentJobId: string | null = null;
  private currentVertexIds: string[] = [];
  private readonly seriesByVertex = new Map<string, WatermarkSample[]>();
  private readonly tick$ = new BehaviorSubject<number>(0);

  private pollSub: Subscription | null = null;
  private refCount = 0;

  private readonly persistEnabled$ = new BehaviorSubject<boolean>(this.readPersistFlag());
  private persistWriteTimer: ReturnType<typeof setTimeout> | null = null;

  public track(jobId: string, vertexIds: readonly string[]): () => void {
    const jobChanged = this.currentJobId !== jobId;
    if (jobChanged) {
      this.currentJobId = jobId;
      this.seriesByVertex.clear();
      this.currentVertexIds = [...vertexIds];
      if (this.persistEnabled$.value) {
        this.hydrateFromStorage(jobId);
      }
      this.tick$.next(this.tick$.value + 1);
    } else {
      const merged = new Set<string>(this.currentVertexIds);
      vertexIds.forEach(id => merged.add(id));
      this.currentVertexIds = Array.from(merged);
    }
    this.refCount++;
    if (!this.pollSub) {
      this.pollSub = this.statusService.refresh$.subscribe(() => this.pollAllVertices());
    }
    let released = false;
    return () => {
      if (released) {
        return;
      }
      released = true;
      this.refCount = Math.max(0, this.refCount - 1);
      if (this.refCount === 0) {
        this.pollSub?.unsubscribe();
        this.pollSub = null;
      }
    };
  }

  public changes$(): Observable<number> {
    return this.tick$.asObservable();
  }

  public persistenceEnabled$(): Observable<boolean> {
    return this.persistEnabled$.asObservable();
  }

  public isPersistenceEnabled(): boolean {
    return this.persistEnabled$.value;
  }

  public setPersistenceEnabled(enabled: boolean): void {
    if (this.persistEnabled$.value === enabled) {
      return;
    }
    this.persistEnabled$.next(enabled);
    this.writePersistFlag(enabled);
    if (enabled) {
      if (this.currentJobId) {
        this.schedulePersistWrite();
      }
    } else {
      this.clearAllStoredData();
    }
  }

  public seriesFor(vertexId: string): readonly WatermarkSample[] {
    return this.seriesByVertex.get(vertexId) ?? [];
  }

  public latestSample(vertexId: string): WatermarkSample | undefined {
    const samples = this.seriesByVertex.get(vertexId);
    return samples && samples.length > 0 ? samples[samples.length - 1] : undefined;
  }

  public allSeries(): ReadonlyMap<string, readonly WatermarkSample[]> {
    return this.seriesByVertex;
  }

  private pollAllVertices(): void {
    const jobId = this.currentJobId;
    if (!jobId || this.currentVertexIds.length === 0) {
      return;
    }
    const now = Date.now();
    const vertexIds = [...this.currentVertexIds];
    forkJoin(
      vertexIds.map(vid =>
        this.metricsService.loadWatermarks(jobId, vid).pipe(
          map(result => ({ vid, lowWatermark: result.lowWatermark })),
          catchError(() => of({ vid, lowWatermark: NaN }))
        )
      )
    ).subscribe(results => {
      results.forEach(({ vid, lowWatermark }) => this.appendSample(vid, now, lowWatermark));
      if (this.persistEnabled$.value) {
        this.schedulePersistWrite();
      }
      this.tick$.next(this.tick$.value + 1);
    });
  }

  private appendSample(vertexId: string, t: number, lowWatermark: number): void {
    const isIdle = isNaN(lowWatermark);
    const lagMs = isIdle ? NaN : Math.max(0, t - lowWatermark);
    const samples = this.seriesByVertex.get(vertexId) ?? [];
    samples.push({ t, lagMs, idle: isIdle });
    const cutoff = t - WatermarkLagHistoryService.RETENTION_MS;
    while (samples.length > 0 && samples[0].t < cutoff) {
      samples.shift();
    }
    this.seriesByVertex.set(vertexId, samples);
  }

  private readPersistFlag(): boolean {
    try {
      return localStorage.getItem(WatermarkLagHistoryService.PERSIST_FLAG_KEY) === 'true';
    } catch {
      return false;
    }
  }

  private writePersistFlag(enabled: boolean): void {
    try {
      localStorage.setItem(WatermarkLagHistoryService.PERSIST_FLAG_KEY, enabled ? 'true' : 'false');
    } catch {
      // Ignore quota / unavailable storage.
    }
  }

  private storageKeyFor(jobId: string): string {
    return WatermarkLagHistoryService.PERSIST_DATA_KEY_PREFIX + jobId;
  }

  private hydrateFromStorage(jobId: string): void {
    let raw: string | null = null;
    try {
      raw = localStorage.getItem(this.storageKeyFor(jobId));
    } catch {
      return;
    }
    if (!raw) {
      return;
    }
    try {
      const parsed = JSON.parse(raw) as Record<string, WatermarkSample[]>;
      const cutoff = Date.now() - WatermarkLagHistoryService.RETENTION_MS;
      for (const [vertexId, samples] of Object.entries(parsed)) {
        if (!Array.isArray(samples)) {
          continue;
        }
        const trimmed = samples.filter(s => s && typeof s.t === 'number' && s.t >= cutoff);
        if (trimmed.length > 0) {
          this.seriesByVertex.set(vertexId, trimmed);
        }
      }
    } catch {
      // Bad payload — drop it.
      try {
        localStorage.removeItem(this.storageKeyFor(jobId));
      } catch {
        /* ignore */
      }
    }
  }

  private schedulePersistWrite(): void {
    if (this.persistWriteTimer !== null) {
      return;
    }
    this.persistWriteTimer = setTimeout(() => {
      this.persistWriteTimer = null;
      this.writeStorage();
    }, WatermarkLagHistoryService.PERSIST_WRITE_DEBOUNCE_MS);
  }

  private writeStorage(): void {
    const jobId = this.currentJobId;
    if (!jobId || !this.persistEnabled$.value) {
      return;
    }
    const payload: Record<string, WatermarkSample[]> = {};
    this.seriesByVertex.forEach((samples, vertexId) => {
      payload[vertexId] = samples;
    });
    try {
      localStorage.setItem(this.storageKeyFor(jobId), JSON.stringify(payload));
    } catch {
      // Quota exceeded or storage unavailable — silently disable to avoid
      // repeated failed writes. The user can re-enable from the UI.
      this.persistEnabled$.next(false);
      this.writePersistFlag(false);
    }
  }

  private clearAllStoredData(): void {
    if (this.persistWriteTimer !== null) {
      clearTimeout(this.persistWriteTimer);
      this.persistWriteTimer = null;
    }
    try {
      const prefix = WatermarkLagHistoryService.PERSIST_DATA_KEY_PREFIX;
      const toRemove: string[] = [];
      for (let i = 0; i < localStorage.length; i++) {
        const key = localStorage.key(i);
        if (key && key.startsWith(prefix)) {
          toRemove.push(key);
        }
      }
      toRemove.forEach(k => localStorage.removeItem(k));
    } catch {
      /* ignore */
    }
  }
}
