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

import {
  HttpErrorResponse,
  HttpEvent,
  HttpHandler,
  HttpInterceptor,
  HttpRequest,
  HttpResponse,
  HttpResponseBase,
  HttpStatusCode
} from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable, throwError, timer } from 'rxjs';
import { catchError, retry, tap } from 'rxjs/operators';

import { StatusService } from '@flink-runtime-web/services';
import { NzNotificationService, NzNotificationDataOptions } from 'ng-zorro-antd/notification';

@Injectable()
export class AppInterceptor implements HttpInterceptor {
  constructor(
    private readonly statusService: StatusService,
    private readonly notificationService: NzNotificationService
  ) {}

  intercept(req: HttpRequest<unknown>, next: HttpHandler): Observable<HttpEvent<unknown>> {
    // Error response from below url should be ignored
    const ignoreErrorUrlEndsList = ['checkpoints/config', 'checkpoints'];
    const ignoreErrorMessage = ['File not found.'];
    const option: NzNotificationDataOptions = {
      nzDuration: 0,
      nzStyle: { width: 'auto', 'white-space': 'pre-wrap' }
    };

    return next.handle(req.clone({ withCredentials: true })).pipe(
      retry({
        count: 3,
        delay: (error: HttpErrorResponse, retryCount) => {
          if (req.method === 'GET' && (error.status === 0 || error.status >= 500)) {
            return timer(Math.pow(2, retryCount) * 1000);
          }
          throw error;
        }
      }),
      tap(event => {
        if (event instanceof HttpResponse) {
          const networkErrorMessage = 'Connection lost or server error. Retrying...';
          const index = this.statusService.listOfErrorMessage.indexOf(networkErrorMessage);
          if (index !== -1) {
            this.statusService.listOfErrorMessage.splice(index, 1);
            this.notificationService.remove();
          }
        }
      }),
      catchError(res => {
        if (
          res instanceof HttpResponseBase &&
          (res.status == HttpStatusCode.MovedPermanently ||
            res.status == HttpStatusCode.TemporaryRedirect ||
            res.status == HttpStatusCode.SeeOther) &&
          res.headers.has('Location')
        ) {
          window.location.href = String(res.headers.get('Location'));
        }

        const errorMessage = res && res.error && res.error.errors && res.error.errors[0];
        if (
          errorMessage &&
          ignoreErrorUrlEndsList.every(url => !res.url.endsWith(url)) &&
          ignoreErrorMessage.every(message => errorMessage !== message)
        ) {
          this.statusService.listOfErrorMessage.push(errorMessage);
          this.notificationService.info('Server Response Message:', errorMessage.replaceAll(' at ', '\n at '), option);
        } else if (res.status === 0 || res.status >= 500) {
          const networkErrorMessage = 'Connection lost or server error. Retrying...';
          if (!this.statusService.listOfErrorMessage.includes(networkErrorMessage)) {
            this.statusService.listOfErrorMessage.push(networkErrorMessage);
            this.notificationService.warning('Network Error:', networkErrorMessage, option);
          }
        }
        return throwError(res);
      })
    );
  }
}
