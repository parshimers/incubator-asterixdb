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
import { ViewEncapsulation, Component, Input, OnInit, ViewChild,  AfterViewInit, ChangeDetectionStrategy, ChangeDetectorRef } from '@angular/core';

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
    selector: 'plan-node',
    templateUrl: 'plan-node.component.html',
    styleUrls: ['plan-node.component.scss'],
    encapsulation: ViewEncapsulation.None,
})

export class PlanNodeComponent {
    @Input() node: any;
    @Input() level;
    @Input() item = 0;
    @Input() subplan = 0;
    @Input() planName = "";
    @Input() viewParams;

    details: any;
    viewParams_: any;

    constructor() {}

    numberOfInputs: 0;

    selected = false;
    viewBox


    ngAfterViewInit() {
        //Adding classes for further granularity/level styling
        //console.log('node'+ this.level + this.item + this.subplan + this.planName);
        var d = document.getElementById('node'+ this.level + this.item + this.subplan + this.planName);
        d.className += " level" + this.level;
        d.className += " item" + this.item;
        d.className += " subplan" + this.subplan;
        d.className += " planName" + this.planName;
    }

    ngOnInit() {
        this.viewParams = NORMAL;

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

    toggleDetails(me) {
        if (this.viewParams.viewMode === 'NORMAL') {
            this.viewParams = FULL;
        } else {
            this.viewParams = NORMAL;
        }

        var ele = document.getElementById('node'+ this.level + this.item + this.subplan + this.planName);
        (<any>ele).style.width = this.viewParams.width;
        (<any>ele).style.height = this.viewParams.height;
        if (this.viewParams.viewMode === "FULL") {
            (<any>ele).style.height = "unset";
            (<any>ele).style.maxHeight = this.viewParams.height;
        }
        var ele2 = document.getElementById('node'+ this.level + this.item + this.subplan + this.planName).getElementsByClassName('details');
        (<any>ele2[0]).style.visibility = this.viewParams.visible;
        (<any>ele2[0]).style.opacity = this.viewParams.opacity;
        var ele3 = document.getElementById('node'+ this.level + this.item + this.subplan + this.planName).getElementsByClassName('details');
        (<any>ele3[0]).style.borderTop = this.viewParams.border;
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

    /*
        getElementById()	Returns the element that has the ID attribute with the specified value
        getElementsByClassName()	Returns a NodeList containing all elements with the specified class name
        getElementsByName()	Returns a NodeList containing all elements with a specified name
        getElementsByTagName()
    changeElement(id) {
        var el = document.getElementById(id);
        el.style.color = "red";
        el.style.fontSize = "15px";
        el.style.backgroundColor = "#FFFFFF";
    }

    changeElementSize(id) {
        var el = document.getElementsByName('plan-node');
        console.log(el)
       // el.style.color = "red";
       // el.style.fontSize = "15px";
       // el.style.backgroundColor = "#FFFFFF";
    }
    */
}