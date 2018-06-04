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
import { Component, Input, OnInit, OnChanges } from '@angular/core';
import { Store } from '@ngrx/store';
import { Observable } from 'rxjs/Observable';
import { isArray } from 'util';

@Component({
    moduleId: module.id,
    selector: 'tree-node',
    templateUrl: 'tree-node.component.html',
    styleUrls: ['tree-node.component.scss'],
})

export class TreeNodeComponent {

    @Input() node: any;
    node_: any;
    final = true;
    visible = false;
    nodeContentKeys: any;
    nodeChildren: any;

    constructor() { this.final = true; }

    initData() {
        this.node_ = this.node;
        this.nodeChildren = this.node.children;
        if (this.node_[0].value) {
            this.nodeContentKeys = Object.keys(this.node_[0].value);
        }
    }

    ngOnChanges() {
       this.initData();
    }

    ngOnInit() {
       this.initData();

        /*
        if (this.node_.length == 0 ) {
            this.final = false;
        } else {
            if(this.node_[0].children) {
                if (this.node_[0].children.length == 0 ) {
                    this.final = false;
                }
            } else {
                this.final = false;
            }
        }*/
    }

    getFinal() {
        return true;
    }

    toggle(){
        this.visible = !this.visible;
    }

    checkIfObject(node) {

        //console.log(typeof node.value)
        //console.log(node.value)

        if (typeof node.value === 'object') {
            console.log('Am an Array !!')
        } else if (typeof node.value === 'object') {
            console.log('Am an Object !!')
        } else {
            console.log('Am a misserable key value !!')
            return true;
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