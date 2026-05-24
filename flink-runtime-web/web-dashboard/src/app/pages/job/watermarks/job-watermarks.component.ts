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

import { DecimalPipe, NgFor, NgIf } from '@angular/common';
import {
  AfterViewInit,
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  ElementRef,
  OnDestroy,
  ViewChild
} from '@angular/core';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

import * as G2 from '@antv/g2';
import { Chart } from '@antv/g2';
import { LagSparklineComponent } from '@flink-runtime-web/components/lag-sparkline/lag-sparkline.component';
import { JobDetailCorrect, NodesItemCorrect, WatermarkSample } from '@flink-runtime-web/interfaces';
import { WatermarkLagHistoryService } from '@flink-runtime-web/services';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzEmptyModule } from 'ng-zorro-antd/empty';
import { NzTagModule } from 'ng-zorro-antd/tag';

import { JobLocalService } from '../job-local.service';

interface VertexCard {
  vertexId: string;
  name: string;
  currentLagMs: number;
  trend: 'up' | 'down' | 'flat' | 'idle';
  status: 'healthy' | 'tracked' | 'elevated' | 'idle';
  values: number[];
}

interface ChartPoint {
  time: number;
  lagSeconds: number;
  vertex: string;
}

@Component({
  selector: 'flink-job-watermarks',
  templateUrl: './job-watermarks.component.html',
  styleUrls: ['./job-watermarks.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [NgIf, NgFor, DecimalPipe, NzCardModule, NzEmptyModule, NzTagModule, LagSparklineComponent]
})
export class JobWatermarksComponent implements AfterViewInit, OnDestroy {
  public cards: VertexCard[] = [];
  public selectedVertexId: string | null = null;
  public hasAnyData = false;
  public thresholdSeconds = 60;

  @ViewChild('chartContainer', { static: true }) private readonly chartContainer: ElementRef<HTMLElement>;

  private chartInstance: Chart | null = null;
  private jobId: string | null = null;
  private vertices: NodesItemCorrect[] = [];
  private trackTeardown: (() => void) | null = null;
  private readonly destroy$ = new Subject<void>();

  constructor(
    private readonly jobLocalService: JobLocalService,
    private readonly history: WatermarkLagHistoryService,
    private readonly cdr: ChangeDetectorRef
  ) {}

  public ngAfterViewInit(): void {
    this.setUpChart();

    this.jobLocalService
      .jobDetailChanges()
      .pipe(takeUntil(this.destroy$))
      .subscribe(job => this.onJobChange(job));

    this.history
      .changes$()
      .pipe(takeUntil(this.destroy$))
      .subscribe(() => this.refreshView());
  }

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
    this.trackTeardown?.();
    this.trackTeardown = null;
    this.chartInstance?.destroy();
    this.chartInstance = null;
  }

  public selectVertex(vertexId: string): void {
    this.selectedVertexId = this.selectedVertexId === vertexId ? null : vertexId;
    this.refreshView();
  }

  public trackByVertexId(_index: number, card: VertexCard): string {
    return card.vertexId;
  }

  private onJobChange(job: JobDetailCorrect): void {
    const nextJobId = job.plan.jid;
    const nextVertices = job.plan.nodes;
    const sameJob = nextJobId === this.jobId;
    const sameVertexSet =
      sameJob &&
      nextVertices.length === this.vertices.length &&
      nextVertices.every((v, i) => v.id === this.vertices[i].id);
    if (sameVertexSet) {
      return;
    }
    this.jobId = nextJobId;
    this.vertices = nextVertices;
    this.trackTeardown?.();
    this.trackTeardown = this.history.track(
      nextJobId,
      nextVertices.map(v => v.id)
    );
    this.refreshView();
  }

  private setUpChart(): void {
    this.chartInstance = new G2.Chart({
      container: this.chartContainer.nativeElement,
      height: 320,
      autoFit: true,
      padding: [20, 24, 60, 60]
    });
    this.chartInstance.scale({
      time: { type: 'time', mask: 'HH:mm:ss', tickCount: 6, nice: false },
      lagSeconds: { alias: 'Lag (s)', min: 0, nice: true },
      vertex: { type: 'cat' }
    });
    this.chartInstance.axis('lagSeconds', { title: { offset: 40 } });
    this.chartInstance.legend('vertex', { position: 'bottom' });
    this.chartInstance.tooltip({ shared: true, showCrosshairs: true });
    this.chartInstance.line().position('time*lagSeconds').color('vertex').shape('smooth').size(1.5).animate(false);
    this.chartInstance.render();
  }

  private refreshView(): void {
    if (!this.jobId || this.vertices.length === 0) {
      this.cards = [];
      this.hasAnyData = false;
      this.chartInstance?.changeData([]);
      this.cdr.markForCheck();
      return;
    }
    const cards: VertexCard[] = [];
    const chartPoints: ChartPoint[] = [];
    let anyData = false;
    for (const vertex of this.vertices) {
      if (this.selectedVertexId && vertex.id !== this.selectedVertexId) {
        continue;
      }
      const series = this.history.seriesFor(vertex.id);
      if (series.length > 0) {
        anyData = true;
      }
      const label = this.shortName(vertex);
      for (const sample of series) {
        if (sample.idle || !Number.isFinite(sample.lagMs)) {
          continue;
        }
        chartPoints.push({ time: sample.t, lagSeconds: sample.lagMs / 1000, vertex: label });
      }
    }
    for (const vertex of this.vertices) {
      cards.push(this.toCard(vertex, this.history.seriesFor(vertex.id)));
    }
    cards.sort((a, b) => {
      const aIdle = a.status === 'idle' || !Number.isFinite(a.currentLagMs);
      const bIdle = b.status === 'idle' || !Number.isFinite(b.currentLagMs);
      if (aIdle !== bIdle) {
        return aIdle ? 1 : -1;
      }
      return (b.currentLagMs || 0) - (a.currentLagMs || 0);
    });
    this.cards = cards;
    this.hasAnyData = anyData;
    if (this.chartInstance) {
      this.chartInstance.changeData(chartPoints);
    }
    this.cdr.markForCheck();
  }

  private toCard(vertex: NodesItemCorrect, samples: readonly WatermarkSample[]): VertexCard {
    const finite = samples.filter(s => Number.isFinite(s.lagMs));
    const latest = finite.length > 0 ? finite[finite.length - 1].lagMs : NaN;
    const idle = samples.length > 0 && samples[samples.length - 1].idle;
    const status: VertexCard['status'] =
      idle || finite.length === 0
        ? 'idle'
        : latest >= this.thresholdSeconds * 1000
        ? 'elevated'
        : latest >= (this.thresholdSeconds * 1000) / 2
        ? 'tracked'
        : 'healthy';
    const trend = this.computeTrend(finite);
    return {
      vertexId: vertex.id,
      name: this.shortName(vertex),
      currentLagMs: latest,
      trend: idle ? 'idle' : trend,
      status,
      values: samples.map(s => (Number.isFinite(s.lagMs) ? s.lagMs : NaN))
    };
  }

  private computeTrend(finite: readonly WatermarkSample[]): 'up' | 'down' | 'flat' {
    if (finite.length < 4) {
      return 'flat';
    }
    const tail = finite.slice(-Math.min(6, finite.length));
    const first = tail[0].lagMs;
    const last = tail[tail.length - 1].lagMs;
    const delta = last - first;
    const ref = Math.max(first, 1000);
    if (delta > ref * 0.1) {
      return 'up';
    }
    if (delta < -ref * 0.1) {
      return 'down';
    }
    return 'flat';
  }

  private shortName(vertex: NodesItemCorrect): string {
    const name = vertex.detail?.name ?? vertex.id;
    return name.length > 48 ? `${name.slice(0, 45)}…` : name;
  }
}
