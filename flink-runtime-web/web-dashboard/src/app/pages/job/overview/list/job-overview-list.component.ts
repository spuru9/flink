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

import { DecimalPipe, NgForOf, NgIf, isPlatformBrowser } from '@angular/common';
import {
  AfterViewInit,
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  ElementRef,
  EventEmitter,
  Inject,
  Input,
  OnDestroy,
  OnInit,
  Output,
  PLATFORM_ID
} from '@angular/core';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

import { HumanizeBytesPipe } from '@flink-runtime-web/components/humanize-bytes.pipe';
import { HumanizeDatePipe } from '@flink-runtime-web/components/humanize-date.pipe';
import { HumanizeDurationPipe } from '@flink-runtime-web/components/humanize-duration.pipe';
import { JobBadgeComponent } from '@flink-runtime-web/components/job-badge/job-badge.component';
import { LagSparklineComponent } from '@flink-runtime-web/components/lag-sparkline/lag-sparkline.component';
import { ResizeComponent } from '@flink-runtime-web/components/resize/resize.component';
import { TaskBadgeComponent } from '@flink-runtime-web/components/task-badge/task-badge.component';
import { NodesItemCorrect } from '@flink-runtime-web/interfaces';
import { JobLocalService } from '@flink-runtime-web/pages/job/job-local.service';
import { StatusService, WatermarkLagHistoryService } from '@flink-runtime-web/services';
import { NzBadgeModule } from 'ng-zorro-antd/badge';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzTableModule, NzTableSortFn } from 'ng-zorro-antd/table';
import { NzTooltipModule } from 'ng-zorro-antd/tooltip';

function createSortFn(
  selector: (item: NodesItemCorrect) => number | string | undefined
): NzTableSortFn<NodesItemCorrect> {
  return (pre, next) => (selector(pre)! > selector(next)! ? 1 : -1);
}

const rescaleTimeout = 2500;

@Component({
  selector: 'flink-job-overview-list',
  templateUrl: './job-overview-list.component.html',
  styleUrls: ['./job-overview-list.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    NzTableModule,
    NgForOf,
    NzTooltipModule,
    JobBadgeComponent,
    NgIf,
    HumanizeBytesPipe,
    DecimalPipe,
    HumanizeDatePipe,
    HumanizeDurationPipe,
    TaskBadgeComponent,
    ResizeComponent,
    NzButtonModule,
    NzIconModule,
    NzBadgeModule,
    LagSparklineComponent
  ]
})
export class JobOverviewListComponent implements OnInit, AfterViewInit, OnDestroy {
  private static readonly END_TIME_MIN_WIDTH = 200; // Minimum space for End Time column

  public readonly trackById = (_: number, node: NodesItemCorrect): string => node.id;
  public readonly webRescaleEnabled = this.statusService.configuration.features['web-rescale'];

  public readonly sortStatusFn = createSortFn(item => item.detail?.status);
  public readonly sortReadBytesFn = createSortFn(item => item.detail?.metrics?.['read-bytes']);
  public readonly sortReadRecordsFn = createSortFn(item => item.detail?.metrics?.['read-records']);
  public readonly sortWriteBytesFn = createSortFn(item => item.detail?.metrics?.['write-bytes']);
  public readonly sortWriteRecordsFn = createSortFn(item => item.detail?.metrics?.['write-records']);
  public readonly sortParallelismFn = createSortFn(item => item.parallelism);
  public readonly sortStartTimeFn = createSortFn(item => item.detail?.['start-time']);
  public readonly sortDurationFn = createSortFn(item => item.detail?.duration);
  public readonly sortEndTimeFn = createSortFn(item => item.detail?.['end-time']);

  public innerNodes: NodesItemCorrect[] = [];
  public left = 390;
  public dynamicResizeMin = 390;
  public tableScrollX = 0;

  public desiredParallelism = new Map<string, number>();

  public rescaleTimeoutId: number | undefined;

  public readonly lagThresholdMs = 60_000;
  private readonly lagValuesCache = new Map<string, number[]>();
  private trackedJobId: string | null = null;
  private trackTeardown: (() => void) | null = null;
  private readonly destroy$ = new Subject<void>();

  @Output() public readonly nodeClick = new EventEmitter<NodesItemCorrect>();

  @Output() public readonly rescale = new EventEmitter<Map<string, number>>();

  @Input() public selectedNode: NodesItemCorrect;

  @Input()
  public set nodes(value: NodesItemCorrect[]) {
    this.innerNodes = value;
    for (const node of value) {
      if (node.parallelism == this.desiredParallelism.get(node.id)) {
        this.desiredParallelism.delete(node.id);
      }
    }
  }

  public get nodes(): NodesItemCorrect[] {
    return this.innerNodes;
  }

  constructor(
    public readonly elementRef: ElementRef,
    private readonly statusService: StatusService,
    private readonly jobLocalService: JobLocalService,
    private readonly watermarkHistory: WatermarkLagHistoryService,
    @Inject(PLATFORM_ID) private platformId: object,
    private readonly cdr: ChangeDetectorRef
  ) {}

  public ngOnInit(): void {
    this.jobLocalService
      .jobDetailChanges()
      .pipe(takeUntil(this.destroy$))
      .subscribe(job => {
        const nextJobId = job.plan.jid;
        const nextVertexIds = job.plan.nodes.map(n => n.id);
        if (this.trackedJobId === nextJobId) {
          return;
        }
        this.trackedJobId = nextJobId;
        this.trackTeardown?.();
        this.lagValuesCache.clear();
        this.trackTeardown = this.watermarkHistory.track(nextJobId, nextVertexIds);
      });
    this.watermarkHistory
      .changes$()
      .pipe(takeUntil(this.destroy$))
      .subscribe(() => {
        this.lagValuesCache.clear();
        this.cdr.markForCheck();
      });
  }

  public ngAfterViewInit(): void {
    if (isPlatformBrowser(this.platformId)) {
      setTimeout(() => this.updateLeftBasedOnScreenSize(), 0);

      window.addEventListener('resize', this.handleWindowResize);
    }
  }

  public ngOnDestroy(): void {
    if (isPlatformBrowser(this.platformId)) {
      window.removeEventListener('resize', this.handleWindowResize);
    }
    this.destroy$.next();
    this.destroy$.complete();
    this.trackTeardown?.();
    this.trackTeardown = null;
  }

  public lagValuesFor(nodeId: string): number[] {
    const cached = this.lagValuesCache.get(nodeId);
    if (cached) {
      return cached;
    }
    const samples = this.watermarkHistory.seriesFor(nodeId);
    const values = samples.map(s => (Number.isFinite(s.lagMs) ? s.lagMs : NaN));
    this.lagValuesCache.set(nodeId, values);
    return values;
  }

  private readonly handleWindowResize = (): void => {
    this.updateLeftBasedOnScreenSize();
  };

  /**
   * Initialize table dimensions
   */
  private updateLeftBasedOnScreenSize(): void {
    this.left = 390;
    this.dynamicResizeMin = 390;

    const tableHeaders = this.elementRef.nativeElement.querySelectorAll('thead th');
    let fixedColumnsWidth = 0;
    let foundRightColumn = false;

    tableHeaders.forEach((th: HTMLElement, index: number) => {
      if (index > 0 && !foundRightColumn) {
        if (th.hasAttribute('nzright')) {
          foundRightColumn = true;
        } else {
          const width = th.getAttribute('nzWidth');
          if (width) {
            fixedColumnsWidth += parseInt(width, 10);
          }
        }
      }
    });

    this.tableScrollX = this.left + fixedColumnsWidth + JobOverviewListComponent.END_TIME_MIN_WIDTH;
    this.cdr.detectChanges();
  }

  public clickNode(node: NodesItemCorrect): void {
    this.nodeClick.emit(node);
  }

  public clickScaleUp(node: NodesItemCorrect): void {
    let currentDesiredParallelism = this.desiredParallelism.get(node.id);
    if (currentDesiredParallelism == undefined) {
      currentDesiredParallelism = node.parallelism;
    }
    const newDesiredParallelism = currentDesiredParallelism + 1;
    this.changeDesiredParallelism(node, newDesiredParallelism);
  }

  public clickScaleDown(node: NodesItemCorrect): void {
    let currentDesiredParallelism = this.desiredParallelism.get(node.id);
    if (currentDesiredParallelism == undefined) {
      currentDesiredParallelism = node.parallelism;
    }
    const newDesiredParallelism = Math.max(1, currentDesiredParallelism - 1);
    this.changeDesiredParallelism(node, newDesiredParallelism);
  }

  private changeDesiredParallelism(node: NodesItemCorrect, newDesiredParallelism: number): void {
    if (newDesiredParallelism == node.parallelism) {
      this.desiredParallelism.delete(node.id);
    } else {
      this.desiredParallelism.set(node.id, newDesiredParallelism);
    }
    if (this.rescaleTimeoutId != undefined) {
      window.clearTimeout(this.rescaleTimeoutId);
    }
    this.rescaleTimeoutId = window.setTimeout(() => {
      if (this.desiredParallelism.size > 0) {
        this.rescale.emit(this.desiredParallelism);
      }
    }, rescaleTimeout);
  }
}
