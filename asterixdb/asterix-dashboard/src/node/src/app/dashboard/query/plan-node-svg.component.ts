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
import { Renderer2, ViewEncapsulation, Component, Input, OnInit, ViewChild,  AfterViewInit, ChangeDetectionStrategy, ChangeDetectorRef } from '@angular/core';

export interface ViewParams {
    viewMode: string,
    width: string,
    height: string,
    visible: string,
    display: string,
    opacity: number,
    border: string
}

export const FULL:ViewParams = {
    viewMode: 'FULL',
    width: '350px',
    height: '180px',
    visible: 'visible',
    display: 'block',
    opacity: 1,
    border: "2px solid #0000FF"
};

export const NORMAL:ViewParams = {
    viewMode: 'NORMAL',
    width: '200px',
    height: '60px',
    visible: 'hidden',
    display: 'none',
    opacity: 0,
    border: "none"
};

@Component({
    moduleId: module.id,
    selector: 'plan-node-svg',
    templateUrl: 'plan-node-svg.component.html',
    styleUrls: ['plan-node-svg.component.scss'],
    encapsulation: ViewEncapsulation.None,
})

export class PlanNodeSVGComponent {
    @Input() node: any;
    @Input() level;
    @Input() item = 0;
    @Input() subplan = 0;
    @Input() planName = "";
    @Input() viewParams;

    details: any;
    viewParams_: any;

    constructor(private renderer: Renderer2) {}

    numberOfInputs: 0;
    selected = false;

    //global: Function;


    ngAfterViewInit() {

       // var mycircle = document.getElementById('node'+ this.level + this.item + this.subplan + this.planName);

       // mycircle.addEventListener('click', function(e) {
       //     console.log('clicked - enlarging: ' + 'node'+ this.level + this.item + this.subplan + this.planName);
       // }, false);

       // let global = this.renderer.listen(mycircle, 'click', (evt) => {
       //     console.log('Clicking the document', evt);
       //     console.log('clicked - enlarging: ' + 'node'+ this.level + this.item + this.subplan + this.planName);
       // })

       // var svg = document.getElementById('mysvg'+ this.level + this.item + this.subplan + this.planName),
       // NS = svg.getAttribute('xmlns');

        // add random circles
        /*var c, i;
        for (i = 0; i < 1000; i++) {
            c = document.createElementNS(NS, 'circle');
            c.setAttributeNS(null, 'cx', Math.round(Math.random() * 100));
            c.setAttributeNS(null, 'cy', Math.round(Math.random() * 100));
            c.setAttributeNS(null, 'r', 1 ); // 20 + Math.round(Math.random() * 30));
            svg.appendChild(c);
        }*/

        /* click a circle
        svg.addEventListener('click', function(e) {
        var t = e.target;
        if (t.nodeName != 'circle') return;
        t.setAttributeNS(null, 'r',
            parseFloat(t.getAttributeNS(null, 'r')) + 10
        );
        console.log(t.getBoundingClientRect());
        }, false); */
          //global();
         // let simple = this.renderer.listen(this.myButton.nativeElement, 'click', (evt) => {
         //   console.log('Clicking the button', evt);
         // });
          //simple();
        //Adding classes for further granularity/level styling
        //console.log('node'+ this.level + this.item + this.subplan + this.planName);
        //var svg = document.getElementById('node'+ this.level + this.item + this.subplan + this.planName);
        //svg.className.baseVal = ... or svg.setAttribute("class", ...) or svg.classList.add(...) or.
        //svg.classList.add("level" + this.level + "item" + this.item);svg.classList.add("subplan" + this.subplan);
        //svg.classList.add("planName" + this.planName);
    }

    ngOnInit() {

        this.viewParams_ = NORMAL;

        /* Some preprocessing to show explanation details */
        if (this.node.inputs){
            this.numberOfInputs = this.node.inputs.length;
        } else {
            this.numberOfInputs = 0;
        }

        if (this.node) {
            let node_=  JSON.parse(JSON.stringify(this.node));

            if (node_.inputs) {
                delete node_['inputs'];
            }

            if (node_.subplan) {
                delete node_['subplan'];
            }

            if (node_.visible != undefined ) {
                delete node_['visible'];
            }

            if (node_.viewDetails != undefined) {
                delete node_['viewDetails'];
            }

            if (node_.operator) {
                delete node_['operator'];
            }

            if (node_.operatorId) {
                delete node_['operatorId'];
            }

            this.details = JSON.stringify(node_, null, 8);

            this.details = this.details.replace(/^{/, '');
            this.details = this.details.replace(/^\n/, '');
            this.details = this.details.replace(/}$/, '');
        }
    }

    ngOnDestroy() {
        /* Not used for the time being
        this.global();
        */
    }

    getNodeName() {
        if(this.node) {
            if (this.node.operator) {
                return (this.node.operator).toUpperCase();
            } else {
                return "NA";
            }
        }
    }

    getNodeOperatorId() {
        if(this.node) {
            if (this.node.operatorId) {
                return (this.node.operatorId).toUpperCase();
            } else {
                return "NA";
            }
        }
    }

    getNodeSubPlan() {
        if(this.node) {
            if (this.node['inputs']) {
                if (this.node['inputs'][this.item]) {
                    if (this.node['inputs'][this.item]['subplan']) {
                        return "Subplan";
                    } else {
                        return "";
                    }
                } else {
                    return "";
                }
            }
        }
    }

    seeDetails(me) {
    /*
        console.log(me);
        console.log(this.viewParams_)

        if (this.viewParams_.viewMode === 'NORMAL') {
            this.viewParams_ = FULL;
            console.log('FULL')
        } else {
            this.viewParams_ = NORMAL;
            console.log('NORMAL')
        }

        var ele = document.getElementById('node'+ this.level + this.item + this.subplan + this.planName);
        (<any>ele).style.width = this.viewParams_.width;
        (<any>ele).style.height = this.viewParams_.height;
        console.log(ele);
        if (this.viewParams_.viewMode === "FULL") {
            (<any>ele).style.height = "unset";
            (<any>ele).style.maxHeight = this.viewParams_.height;
        }
        var ele2 = document.getElementById('node'+ this.level + this.item + this.subplan + this.planName).getElementsByClassName('operation-details');
        console.log(ele2);

        (<any>ele2[0]).style.visibility = this.viewParams_.visible;
        (<any>ele2[0]).style.opacity = this.viewParams_.opacity;

        var ele3 = document.getElementById('node'+ this.level + this.item + this.subplan + this.planName).getElementsByClassName('operation-details');
        (<any>ele3[0]).style.borderTop = this.viewParams_.border;
        console.log(ele3); */
    }

    checkSubPlan() {
        if(this.node) {
            if (this.node['inputs']) {
                if (this.node['inputs'][this.item]) {
                    if (this.node['inputs'][this.item]['subplan']) {
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }
    }
}