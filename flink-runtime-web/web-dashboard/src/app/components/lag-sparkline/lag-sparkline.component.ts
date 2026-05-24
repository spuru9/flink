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

import { NgIf } from '@angular/common';
import { ChangeDetectionStrategy, Component, Input, OnChanges } from '@angular/core';

@Component({
  selector: 'flink-lag-sparkline',
  templateUrl: './lag-sparkline.component.html',
  styleUrls: ['./lag-sparkline.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [NgIf]
})
export class LagSparklineComponent implements OnChanges {
  @Input() public values: readonly number[] = [];
  @Input() public width = 80;
  @Input() public height = 18;
  @Input() public thresholdMs = 60_000;
  @Input() public ariaLabel = 'lag sparkline';

  public points = '';
  public alertBandY: number | null = null;
  public isAlerting = false;
  public hasData = false;

  public ngOnChanges(): void {
    this.recompute();
  }

  private recompute(): void {
    const finiteValues = this.values.filter(v => Number.isFinite(v));
    this.hasData = finiteValues.length > 0;
    if (!this.hasData) {
      this.points = '';
      this.alertBandY = null;
      this.isAlerting = false;
      return;
    }
    const max = Math.max(this.thresholdMs * 1.1, ...finiteValues);
    const min = 0;
    const n = this.values.length;
    const dx = n > 1 ? this.width / (n - 1) : 0;
    const scaleY = (v: number): number => {
      if (!Number.isFinite(v)) {
        return this.height;
      }
      const t = (v - min) / Math.max(1, max - min);
      return this.height - t * this.height;
    };
    this.points = this.values.map((v, i) => `${(i * dx).toFixed(2)},${scaleY(v).toFixed(2)}`).join(' ');
    this.alertBandY = scaleY(this.thresholdMs);
    const latest = finiteValues[finiteValues.length - 1];
    this.isAlerting = latest >= this.thresholdMs;
  }
}
