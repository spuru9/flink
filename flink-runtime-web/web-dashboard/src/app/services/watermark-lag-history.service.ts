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
 * Rolling-window history of watermark lag per vertex. Lag is computed
 * client-side as `Date.now() - lowWatermark`; an idle vertex (where
 * `lowWatermark` is unset / Long.MIN_VALUE) is recorded with `idle = true`
 * and `lagMs = NaN` so the chart can render a gap rather than infinite lag.
 *
 * Self-polls via `statusService.refresh$` while at least one consumer is
 * tracking. When the last consumer releases, polling halts.
 */
@Injectable({ providedIn: 'root' })
export class WatermarkLagHistoryService {
  private static readonly RETENTION_MS = 30 * 60 * 1000;

  private readonly metricsService = inject(MetricsService);
  private readonly statusService = inject(StatusService);

  private currentJobId: string | null = null;
  private currentVertexIds: string[] = [];
  private readonly seriesByVertex = new Map<string, WatermarkSample[]>();
  private readonly tick$ = new BehaviorSubject<number>(0);

  private pollSub: Subscription | null = null;
  private refCount = 0;

  /**
   * Start tracking watermark lag for the given vertices. Returns a teardown
   * function the caller must invoke when finished (typically in ngOnDestroy).
   * Multiple callers are supported; polling stops only when the last one
   * releases.
   */
  public track(jobId: string, vertexIds: readonly string[]): () => void {
    if (this.currentJobId !== jobId) {
      this.currentJobId = jobId;
      this.seriesByVertex.clear();
      this.currentVertexIds = [...vertexIds];
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
}
