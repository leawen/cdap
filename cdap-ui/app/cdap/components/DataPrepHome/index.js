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

import React, { Component, PropTypes } from 'react';
import DataPrep from 'components/DataPrep';
import Helmet from 'react-helmet';
import T from 'i18n-react';
import MyDataPrepApi from 'api/dataprep';
import NamespaceStore from 'services/NamespaceStore';
import {Redirect} from 'react-router-dom';

/**
 *  Routing container for DataPrep for React
 **/
export default class DataPrepHome extends Component {
  constructor(props) {
    super(props);

    this.state = {
      isEmpty: false,
      rerouteTo: null,
      loading: false
    };

    this.namespace = NamespaceStore.getState().selectedNamespace;
  }

  componentWillReceiveProps(nextProps) {
    this.setState({
      rerouteTo: null,
      isEmpty: false
    });
    this.checkWorkspaceId(nextProps);
  }

  checkWorkspaceId(props) {
    if (!props.match.params.workspaceId) {
      this.setState({loading: true});
      let namespace = NamespaceStore.getState().selectedNamespace;

      MyDataPrepApi.getWorkspaceList({ namespace })
        .subscribe((res) => {
          if (res.values.length === 0) {
            this.setState({isEmpty: true, loading: false});
            return;
          }

          this.setState({rerouteTo: res.values[0].id, loading: false});
        });
    }
  }

  render() {
    if (this.state.isEmpty) {
      return (
        <Redirect to={`/ns/${this.namespace}/connections/browser`} />
      );
    }

    if (this.state.rerouteTo) {
      return (
        <Redirect to={`/ns/${this.namespace}/dataprep/${this.state.rerouteTo}`} />
      );
    }

    if (this.state.loading) {
      return null;
    }

    return (
      <div>
        <Helmet
          title={T.translate('features.DataPrep.pageTitle')}
        />
        <DataPrep
          workspaceId={this.props.match.params.workspaceId}
        />
      </div>
    );
  }
}

DataPrepHome.propTypes = {
  match: PropTypes.shape({
    params: PropTypes.shape({
      workspaceId: PropTypes.string
    })
  })
};
