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
require('../../DataPrepTable/DataPrepTable.scss');
require('./CutDirective.scss');
const HEIGHT_OF_THEADER = 41;

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
  render() {
    let {width, left, right, top, bottom} = this.state.columnDimension;
    let {data} = DataPrepStore.getState().dataprep;
    let column = this.props.columns[0];
    data = data.map(d => d[column]);
    let divStyles = {
      width: `${width}px`,
      right: `${right}px`,
      left: `${left}px`,
      top: `${top}px`,
      bottom: `${bottom}px`,
      position: 'fixed'
    };
    let {height: heightOfTableBody} = document.getElementById('dataprep-table-id').getBoundingClientRect();
    return (
      <div
        style={divStyles}
        className="cut-directive dataprep-table"
      >
        <table className="table table-bordered">
          <colgroup>
            <col />
          </colgroup>
          <thead className="thead-inverse">
            <tr>
              <th>
                {column}
              </th>
            </tr>
          </thead>
          <tbody style={{height: heightOfTableBody - HEIGHT_OF_THEADER}}>
              {
                data.map((d, i) => {
                  return (
                    <tr key={i}>
                      <td> {d} </td>
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
  columns: PropTypes.arrayOf(PropTypes.string)
};
