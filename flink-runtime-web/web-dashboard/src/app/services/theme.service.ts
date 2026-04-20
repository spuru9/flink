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

import { DOCUMENT } from '@angular/common';
import { Injectable, inject } from '@angular/core';
import { BehaviorSubject, Observable } from 'rxjs';

export type Theme = 'light' | 'dark';

const STORAGE_KEY = 'flink-dashboard-theme';
const DARK_LINK_ID = 'flink-dark-theme';
const DARK_HREF = 'dark-theme.css';

@Injectable({ providedIn: 'root' })
export class ThemeService {
  private readonly document = inject(DOCUMENT);
  private readonly theme$ = new BehaviorSubject<Theme>(this.resolveInitialTheme());

  public readonly themeChanges$: Observable<Theme> = this.theme$.asObservable();

  public init(): void {
    this.applyTheme(this.theme$.value);
  }

  public getTheme(): Theme {
    return this.theme$.value;
  }

  public getMonacoTheme(): 'vs' | 'vs-dark' {
    return this.theme$.value === 'dark' ? 'vs-dark' : 'vs';
  }

  public getG2Theme(): 'default' | 'dark' {
    return this.theme$.value === 'dark' ? 'dark' : 'default';
  }

  public toggle(): void {
    this.setTheme(this.theme$.value === 'dark' ? 'light' : 'dark');
  }

  public setTheme(theme: Theme): void {
    if (theme === this.theme$.value) {
      return;
    }
    this.theme$.next(theme);
    this.applyTheme(theme);
    try {
      this.document.defaultView?.localStorage.setItem(STORAGE_KEY, theme);
    } catch {
      // Storage may be unavailable (private mode, disabled cookies); ignore.
    }
  }

  private resolveInitialTheme(): Theme {
    try {
      const stored = this.document.defaultView?.localStorage.getItem(STORAGE_KEY);
      if (stored === 'dark' || stored === 'light') {
        return stored;
      }
    } catch {
      // Fall through to system preference.
    }
    const prefersDark = this.document.defaultView?.matchMedia?.('(prefers-color-scheme: dark)').matches;
    return prefersDark ? 'dark' : 'light';
  }

  private applyTheme(theme: Theme): void {
    const body = this.document.body;
    body.classList.toggle('flink-theme-dark', theme === 'dark');
    body.classList.toggle('flink-theme-light', theme === 'light');

    if (theme === 'dark') {
      this.ensureDarkStylesheet();
    } else {
      this.removeDarkStylesheet();
    }

    // Monaco is lazy-loaded; setTheme is a no-op until an editor has rendered.
    // Editor components re-apply the current theme in their init callback to
    // cover the "editor created after toggle" case.
    const monaco = (this.document.defaultView as unknown as { monaco?: { editor?: { setTheme(t: string): void } } })
      ?.monaco;
    monaco?.editor?.setTheme(theme === 'dark' ? 'vs-dark' : 'vs');
  }

  private ensureDarkStylesheet(): void {
    if (this.document.getElementById(DARK_LINK_ID)) {
      return;
    }
    const link = this.document.createElement('link');
    link.id = DARK_LINK_ID;
    link.rel = 'stylesheet';
    link.href = DARK_HREF;
    this.document.head.appendChild(link);
  }

  private removeDarkStylesheet(): void {
    const link = this.document.getElementById(DARK_LINK_ID);
    link?.parentNode?.removeChild(link);
  }
}
