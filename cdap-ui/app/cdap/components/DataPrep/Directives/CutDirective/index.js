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

require('../../DataPrepTable/DataPrepTable.scss');
require('./CutDirective.scss');
const cellHighlightClassname = 'cl-highlight';

export default class CutDirective extends Component {
  constructor(props) {
    super(props);
    this.state = {
      columnDimension: {}
    };
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
        if (e.target.className.indexOf(cellHighlightClassname) === -1) {
          if (this.props.onClose) {
            this.props.onClose();
          }
        }
      });
  }
  componentWillUnmount() {
    if (this.documentClick$) {
      this.documentClick$.dispose();
    }
  }
  render() {
    let {data, headers} = DataPrepStore.getState().dataprep;
    let column = this.props.columns[0];

    const renderTableCell = (row, head, highlightColumn) => {
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
        >
          <div
            className={cellHighlightClassname}
          >
            {row[head]}
          </div>
        </td>
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
                  return (
                    <th className={classnames({
                      'gray-out': head !== column
                    })}>
                      <div className={classnames({
                          'gray-out': head !== column
                        })}>
                        {head}
                      </div>
                    </th>
                  );
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
                          return renderTableCell(row, head, column);
                        })
                      }
                    </tr>
                  );
                })
              }
          </tbody>
        </table>
      </div>
    );
  }
}

CutDirective.propTypes = {
  columns: PropTypes.arrayOf(PropTypes.string),
  onClose: PropTypes.func
};
