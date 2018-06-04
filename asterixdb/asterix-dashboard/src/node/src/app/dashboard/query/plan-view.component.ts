import { Component, OnInit, Input, SimpleChange } from '@angular/core';
import { ViewParams, NORMAL, FULL } from './plan-node.component';

export  interface planCount {
    nodesCnt: number,
    levelsCnt: number
}

@Component({
    selector: 'plan-view',
    templateUrl: 'plan-view.component.html',
    styleUrls: ['plan-view.component.scss'],
})

export class PlanViewComponent {

    @Input() plan: any;
    @Input() planName: any;
    @Input() jsonPlan: any;

    plan_: any;
    numberOfLevels: number = 0;
    numberOfNodes: number = 0;
    jsonVisible = false;
    viewParams: ViewParams = NORMAL;

    constructor() {}

    ngOnInit() {
        this.plan_ = this.plan;
        /* find the number of nodes in the tree */
        let summary : planCount = {nodesCnt:0, levelsCnt:0}
        summary = this.analyzePlan(this.plan_, summary);
        this.numberOfLevels = summary.levelsCnt;
        this.numberOfNodes = summary.nodesCnt;
    }

    ngOnChanges(changes: SimpleChange) {
        this.plan_ = this.plan;
        /* find the number of nodes in the tree */
        let summary : planCount = {nodesCnt:0, levelsCnt:0}
        summary = this.analyzePlan(this.plan_, summary);
        this.numberOfLevels = summary.levelsCnt;
        this.numberOfNodes = summary.nodesCnt;
	}

    /*
    * See the JSON contents inside of each node
    */
    showJSON() {
        this.jsonVisible = !this.jsonVisible;
    }

    /* Counts the number of nodes/operations in the tree
    *
    */
    analyzePlan(plan, planCounter) {
        planCounter.nodesCnt += 1;
        planCounter.levelsCnt += 1;
        let nodes = {}
        nodes = plan;
        // augment
        if (nodes) {
            nodes['visible'] = true;
            nodes['viewDetails'] = false;
            if (nodes['inputs']) {
                for (let i = 0; i< nodes['inputs'].length; i++)
                {
                    planCounter = this.analyzePlan(nodes['inputs'][i], planCounter);
                }
            }
        }

        return planCounter;
    }

    /*
    * See the JSON contents inside of each node, with pre-format
    * Not used in this version
    */
    toggleViewDetails(plan) {
        let nodes = {}
        nodes = plan;
        // augment
        nodes['visible'] = true;
        nodes['viewDetails'] = !nodes['viewDetails'];
        if (nodes['inputs']) {
            for (let i = 0; i< nodes['inputs'].length; i++)
            {
                this.toggleViewDetails(nodes['inputs'][i]);
            }
        }
    }

    /*
    * See the JSON contents inside of each node, with pre-format
    * Not used in this version
    */
    seeDetails() {
        if (this.viewParams.viewMode === 'NORMAL') {
            this.viewParams = FULL;
        } else {
            this.viewParams = NORMAL;
        }

        var ele = document.getElementById(this.planName).getElementsByTagName('plan-node')
        for(let i = 0; i < ele.length; i++) {
            (<any>ele[i]).childNodes[1].style.width = this.viewParams.width;
            (<any>ele[i]).childNodes[1].style.height = "unset";
            (<any>ele[i]).childNodes[1].style.maxHeight = this.viewParams.height;
        }

        var ele2 = document.getElementById(this.planName).getElementsByClassName('details');
        for(let i = 0; i < ele2.length; i++) {
            (<any>ele2[i]).style.visibility = this.viewParams.visible;
           (<any>ele2[i]).style.opacity = this.viewParams.opacity;
        }

        var ele3 = document.getElementById(this.planName).getElementsByClassName('details');
        for(let i = 0; i < ele3.length; i++) {
            (<any>ele3[i]).style.borderTop = this.viewParams.border;
        }
    }
}
