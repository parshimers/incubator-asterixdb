/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
import { Injectable } from '@angular/core';
import { Action } from '@ngrx/store';
import { Effect, Actions } from '@ngrx/effects';
import { Observable } from 'rxjs/Observable';
import { of } from 'rxjs/observable/of';
import * as appActions from '../actions/app.actions';
import { SQLService } from '../services/async-query.service';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/switchMap';
import 'rxjs/add/operator/catch';

export type Action = appActions.All

@Injectable()
export class AppEffects {
    constructor(private actions: Actions) {}

    //@Effect()
    //setQueryIndex$: Observable<Action> = this.actions
    //    .ofType(appActions.APP_QUERY_INPUT_INDEX)
    //    .switchMap(query =>
    //        return this.sqlService.selectDatasets()
    //            .map(dataset => new datasetActions.SelectDatasetsSuccess(dataset))
    //            .catch(err => of(new datasetActions.SelectDatasetsFail(err)));
    //})
}