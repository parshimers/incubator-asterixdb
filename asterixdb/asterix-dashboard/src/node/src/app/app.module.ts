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
import { NgModule } from '@angular/core';
import { AppComponent } from './app.component';
import { AppEffects } from './shared/effects/app.effects';
import { BrowserModule } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { HttpClientModule } from '@angular/common/http';
import { EffectsModule } from '@ngrx/effects';
import { DataverseEffects } from './shared/effects/dataverse.effects';
import { DatasetEffects } from './shared/effects/dataset.effects';
import { DatatypeEffects } from './shared/effects/datatype.effects';
import { IndexEffects } from './shared/effects/index.effects';
import { SQLQueryEffects } from './shared/effects/query.effects';
import { MetadataEffects } from './shared/effects/metadata.effects';
import { AppBarComponent }  from './dashboard/appbar.component';
import { DialogMetadataInspector, MetadataComponent }  from './dashboard/query/metadata.component';
import { QueryContainerComponent }  from './dashboard/query/query-container.component';
import { InputQueryComponent }  from './dashboard/query/input.component';
import { QueryOutputComponent }  from './dashboard/query/output.component';
import { AppTabComponent }  from './dashboard/apptab.component';
import { KeysPipe } from './shared/pipes/keys.pipe';
import { ObjectTypePipe } from './shared/pipes/objectType.pipe';
import { ObjectArrayTypePipe } from './shared/pipes/objectArrayType.pipe';
import { reducers } from './shared/reducers';
import { SQLService } from './shared/services/async-query.service'
import { AppCoreService } from './shared/services/app-core.service'
import { MetadataService } from './shared/services/async-metadata.service'
import { DBModule } from '@ngrx/db';
import { FormsModule } from '@angular/forms';
import { MaterialModule } from './material.module';
import { StoreModule,  } from '@ngrx/store';
import { StoreDevtoolsModule } from '@ngrx/store-devtools';
import { schema } from './db';
import { PlanViewComponent } from './dashboard/query/plan-view.component';
import { PlanNodeComponent } from './dashboard/query/plan-node.component';
import { PlanNodeSVGComponent } from './dashboard/query/plan-node-svg.component';
import { TreeNodeComponent } from './dashboard/query/tree-node.component';
import { TreeViewComponent } from './dashboard/query/tree-view.component';
@NgModule({
    declarations: [
        AppComponent,
        AppBarComponent,
       InputQueryComponent,
        QueryOutputComponent,
        KeysPipe,
        MetadataComponent,
        QueryContainerComponent,
        AppTabComponent,
        DialogMetadataInspector,
        ObjectTypePipe,
        ObjectArrayTypePipe,
        PlanNodeComponent,
        PlanNodeSVGComponent,
        PlanViewComponent,
        TreeNodeComponent,
        TreeViewComponent,
    ],
    imports: [
        FormsModule,
        BrowserModule,
        BrowserAnimationsModule,
        DBModule.provideDB(schema),
        EffectsModule.forRoot([AppEffects, MetadataEffects, DataverseEffects, DatasetEffects, DatatypeEffects, IndexEffects, SQLQueryEffects]),
        HttpClientModule,
        MaterialModule,
        StoreModule.forRoot(reducers),
        StoreDevtoolsModule.instrument({
            maxAge: 10
        })
    ],
    entryComponents: [
        DialogMetadataInspector
    ],
    providers: [AppCoreService, SQLService, MetadataService],
    bootstrap: [AppComponent]
})
export class AppModule {}