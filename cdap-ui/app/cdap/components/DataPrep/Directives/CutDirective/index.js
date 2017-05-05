/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
*/

import React, {Component, PropTypes} from 'react';
import DataPrepStore from 'components/DataPrep/store';
import classnames from 'classnames';
import shortid from 'shortid';
import Rx from 'rx';
import { Popover, PopoverTitle, PopoverContent } from 'reactstrap';
import Mousetrap from 'mousetrap';
import isNil from 'lodash/isNil';
import {execute} from 'components/DataPrep/store/DataPrepActionCreator';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';

require('../../DataPrepTable/DataPrepTable.scss');
require('./CutDirective.scss');
const cellHighlightClassname = 'cl-highlight';

export default class CutDirective extends Component {
  constructor(props) {
    super(props);
    this.state = {
      columnDimension: {},
      textSelectionRange: {start: null, end: null, index: null},
      showPopover: false,
      newColName: null
    };

    this.mouseDownHandler = this.mouseDownHandler.bind(this);
    this.togglePopover = this.togglePopover.bind(this);
    this.mouseUpHandler = this.mouseUpHandler.bind(this);
    this.preventPropagation = this.preventPropagation.bind(this);
    this.handleColNameChange = this.handleColNameChange.bind(this);
    this.applyDirective = this.applyDirective.bind(this);
  }
  componentWillMount() {
    if (this.props.columns.length) {
      let column = this.props.columns[0];
      let ele = document.getElementById(`column-${column}`);
      this.setState({
        columnDimension: ele.getBoundingClientRect()
      });
    }
  }
  componentDidMount() {
    this.documentClick$ = Rx.DOM.fromEvent(document.body, 'click', false)
      .subscribe((e) => {
        if (e.target.className.indexOf(cellHighlightClassname) === -1 && ['TR', 'TBODY'].indexOf(e.target.nodeName) === -1) {
          if (this.props.onClose) {
            this.props.onClose();
          }
        }
      });
    let highlightedHeader = document.getElementById('highlighted-header');
    if (highlightedHeader) {
      highlightedHeader.scrollIntoView();
    }
  }
  componentDidUpdate() {
    if (this.tetherRef) {
      this.tetherRef.position();
    }
    let highlightpopover = document.querySelector('.highlight-popover-element');
    if (highlightpopover) {
      this.mouseStrap = new Mousetrap(highlightpopover);
      this.mouseStrap.bind('enter', this.applyDirective);
    } else if (this.mouseStrap) {
      this.mouseStrap.reset();
    }
  }
  componentWillUnmount() {
    if (this.documentClick$) {
      this.documentClick$.dispose();
    }
    if (this.mouseStrap) {
      this.mouseStrap.reset();
    }
  }
  applyDirective() {
    let {start, end} = this.state.textSelectionRange;
    if (!isNil(start) && !isNil(end)) {
      let directive = `cut-character ${this.props.columns[0]} ${this.state.newColName} -c ${start} ${end}`;
      execute([directive])
        .subscribe(() => {
          this.props.onClose();
        }, (err) => {
          console.log('error', err);

          DataPrepStore.dispatch({
            type: DataPrepActions.setError,
            payload: {
              message: err.message || err.response.message
            }
          });
        });
    }
  }
  handleColNameChange(e) {
    this.setState({
      newColName: e.target.value
    });
  }
  togglePopover() {
    if (this.state.showPopover) {
      this.setState({
        showPopover: false,
        textSelectionRange: {start: null, end: null, index: null},
        newColName: null
      });
    }
  }
  mouseDownHandler() {
    this.textSelection = true;
    if (this.state.showPopover) {
      this.togglePopover();
    }
  }
  mouseUpHandler(head, index) {

    let currentSelection = window.getSelection().toString();
    let startRange, endRange;

    if (this.textSelection && currentSelection.length) {
      startRange = window.getSelection().getRangeAt(0).startOffset;
      endRange = window.getSelection().getRangeAt(0).endOffset;
      this.textSelection = false;
      this.setState({
        showPopover: true,
        textSelectionRange: {
          start: startRange,
          end: endRange,
          index
        },
        newColName: this.props.columns[0] + '_copy'
      });
    } else {
      if (this.state.showPopover) {
        this.togglePopover();
      }
    }
  }
  preventPropagation(e) {
    e.stopPropagation();
    e.nativeEvent.stopImmediatePropagation();
    e.preventDefault();
  }
  render() {
    let {data, headers} = DataPrepStore.getState().dataprep;
    let column = this.props.columns[0];

    const renderTableCell = (row, index, head, highlightColumn) => {
      if (head !== highlightColumn) {
        return (
          <td
            key={shortid.generate()}
            className="gray-out"
          >
            <div className="gray-out">
              {row[head]}
            </div>
          </td>
        );
      }
      return (
        <td
          key={shortid.generate()}
          className={cellHighlightClassname}
          onMouseDown={this.mouseDownHandler}
          onMouseUp={this.mouseUpHandler.bind(this, head, index)}
        >
          <div
            className={cellHighlightClassname}
          >
            {
              index === this.state.textSelectionRange.index ?
                (
                  <span>
                    <span>
                      {
                        row[head].slice(0, this.state.textSelectionRange.start)
                      }
                    </span>
                    <span id={`highlight-cell-${index}`}>
                      {
                        row[head].slice(this.state.textSelectionRange.start, this.state.textSelectionRange.end)
                      }
                    </span>
                    <span>
                      {
                        row[head].slice(this.state.textSelectionRange.end)
                      }
                    </span>
                  </span>
                )
              :
                row[head]
            }
          </div>
        </td>
      );
    };

    const renderTableHeader = (head) => {
      if (head !== column) {
        return (
          <th className="gray-out">
            <div className="gray-out">
              {head}
            </div>
          </th>
        );
      }

      return (
        <th id="highlighted-header">
          <div>
            {head}
          </div>
        </th>
      );
    };
    return (
      <div
        id="cut-directive"
        className="cut-directive dataprep-table"
      >
        <table className="table table-bordered">
          <colgroup>
            {
              headers.map(head => {
                return (
                  <col className={classnames({
                    "highlight-column": head === column
                  })} />
                );
              })
            }
          </colgroup>
          <thead className="thead-inverse">
            <tr>
              {
                headers.map( head => {
                  return renderTableHeader(head);
                })
              }
            </tr>
          </thead>
          <tbody>
              {
                data.map((row, i) => {
                  return (
                    <tr key={i}>
                      {
                        headers.map((head) => {
                          return renderTableCell(row, i, head, column);
                        })
                      }
                    </tr>
                  );
                })
              }
          </tbody>
        </table>
        <Popover
          placement="bottom left"
          className="cut-directive-popover"
          isOpen={this.state.showPopover}
          target={`highlight-cell-${this.state.textSelectionRange.index}`}
          toggle={this.togglePopover}
          tether={{
            classPrefix: 'highlight-popover',
            attachment: 'top right',
            targetAttachment: 'bottom left',
            constraints: [
              {
                to: 'scrollParent',
                attachment: 'together'
              }
            ]
          }}
          tetherRef={(ref) => this.tetherRef = ref}
        >
          <PopoverTitle className={cellHighlightClassname}>Extract Using Position</PopoverTitle>
          <PopoverContent
            className={cellHighlightClassname}
            onClick={this.preventPropagation}
          >
            <span className={cellHighlightClassname}>
              Extract characters {this.state.textSelectionRange.start}-{this.state.textSelectionRange.end} from this column to a new column
            </span>
            <div className="col-input-container">
              <strong className={cellHighlightClassname}>Name of destination column</strong>
              <input
                className={classnames("form-control mousetrap", cellHighlightClassname)}
                value={this.state.newColName}
                onChange={this.handleColNameChange}
                autoFocus
              />
            </div>
            <div className="btn btn-primary">
              Apply
            </div>
            <div className="btn">
              Exit 'Extract' mode
            </div>
          </PopoverContent>
        </Popover>
      </div>
    );
  }
}

CutDirective.propTypes = {
  columns: PropTypes.arrayOf(PropTypes.string),
  onClose: PropTypes.func
};
