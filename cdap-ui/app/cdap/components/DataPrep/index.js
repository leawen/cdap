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
import DataPrepTopPanel from 'components/DataPrep/TopPanel';
import DataPrepTable from 'components/DataPrep/DataPrepTable';
import DataPrepSidePanel from 'components/DataPrep/DataPrepSidePanel';
import DataPrepCLI from 'components/DataPrep/DataPrepCLI';
import DataPrepLoading from 'components/DataPrep/DataPrepLoading';
import DataPrepErrorAlert from 'components/DataPrep/DataPrepErrorAlert';
import MyDataPrepApi from 'api/dataprep';
import cookie from 'react-cookie';
import DataPrepStore from 'components/DataPrep/store';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import DataPrepServiceControl from 'components/DataPrep/DataPrepServiceControl';
import ee from 'event-emitter';
import NamespaceStore from 'services/NamespaceStore';
import {MyArtifactApi} from 'api/artifact';
import {findHighestVersion} from 'services/VersionRange/VersionUtilities';
import Version from 'services/VersionRange/Version';
import {setWorkspace} from 'components/DataPrep/store/DataPrepActionCreator';
require('./DataPrep.scss');

/**
 *  Data Prep requires a container component (DataPrepHome) that will handle routing within React.
 *  This is beacause DataPrep component will be included in Pipelines.
 **/
export default class DataPrep extends Component {
  constructor(props) {
    super(props);

    this.state = {
      backendDown: false,
      loading: true,
      onSubmitError: null
    };

    this.toggleBackendDown = this.toggleBackendDown.bind(this);
    this.eventEmitter = ee(ee);

    this.eventEmitter.on('DATAPREP_BACKEND_DOWN', this.toggleBackendDown);
    this.eventEmitter.on('REFRESH_DATAPREP', () => {
      this.setState({
        loading: true
      });
      /*
        Not sure if this is necessary but added it is safer when doing an upgrade.
        - Modified directives?
        - Modified API calls that are not compatible with earlier version?
      */
      DataPrepStore.dispatch({
        type: DataPrepActions.reset
      });
      let workspaceId = this.props.singleWorkspaceMode ? this.props.workspaceId : cookie.load('DATAPREP_WORKSPACE');
      this.setCurrentWorkspace(workspaceId);
      setTimeout(() => {
        this.setState({
          loading: false
        });
      });
    });
  }

  componentWillMount() {
    let workspaceId = this.props.singleWorkspaceMode ? this.props.workspaceId : cookie.load('DATAPREP_WORKSPACE');
    let namespace = NamespaceStore.getState().selectedNamespace;

    MyArtifactApi.list({ namespace })
      .combineLatest(MyDataPrepApi.getApp({ namespace }))
      .subscribe((res) => {
        let wranglerArtifactVersions = res[0].filter((artifact) => {
          return artifact.name === 'wrangler-service';
        }).map((artifact) => {
          return artifact.version;
        });

        let highestVersion = findHighestVersion(wranglerArtifactVersions);
        let currentAppArtifactVersion = new Version(res[1].artifact.version);

        if (highestVersion.compareTo(currentAppArtifactVersion) === 1) {
          DataPrepStore.dispatch({
            type: DataPrepActions.setHigherVersion,
            payload: {
              higherVersion: highestVersion.toString()
            }
          });
        }

        if (this.props.singleWorkspaceMode) {
          DataPrepStore.dispatch({
            type: DataPrepActions.setWorkspaceMode,
            payload: {
              singleWorkspaceMode: true
            }
          });
        }
      });
      this.setCurrentWorkspace(workspaceId);
  }

  componentWillUnmount() {
    if (this.props.onSubmit) {
      let workspaceId = DataPrepStore.getState().dataprep.workspaceId;
      this.props.onSubmit({workspaceId});
    }
    DataPrepStore.dispatch({
      type: DataPrepActions.reset
    });
    this.eventEmitter.off('DATAPREP_BACKEND_DOWN', this.toggleBackendDown);
  }

  setCurrentWorkspace(workspaceId) {
    setWorkspace(workspaceId)
      .subscribe(() => {
        this.setState({loading: false});
      }, (err) => {
        this.setState({loading: false});
        if (err.statusCode === 503) {
          console.log('backend not started');
          this.eventEmitter.emit('DATAPREP_BACKEND_DOWN');
          return;
        }

        cookie.remove('DATAPREP_WORKSPACE', { path: '/' });

        DataPrepStore.dispatch({
          type: DataPrepActions.setInitialized
        });
        this.eventEmitter.emit('DATAPREP_NO_WORKSPACE_ID');
      });
  }

  toggleBackendDown() {
    this.setState({backendDown: true});
  }

  onServiceStart() {
    this.setState({backendDown: false});
    let workspaceId = this.props.singleWorkspaceMode ? this.props.workspaceId : cookie.load('DATAPREP_WORKSPACE');
    this.setCurrentWorkspace(workspaceId);
  }

  renderBackendDown() {
    return (
      <DataPrepServiceControl
        onServiceStart={this.onServiceStart.bind(this)}
      />
    );
  }

  onSubmitToListener({workspaceId, directives, schema}) {
    if (!this.props.onSubmit) {
      return;
    }
    this.props.onSubmit({
      workspaceId,
      directives,
      schema: schema
    });
  }

  render() {
    if (this.state.backendDown) { return this.renderBackendDown(); }

    if (this.state.loading) {
      return (
        <div className="dataprep-container">
          <h3 className="text-xs-center">
            <span className="fa fa-spin fa-spinner" />
          </h3>
        </div>
      );
    }

    return (
      <div className="dataprep-container">
        <DataPrepErrorAlert />
        <DataPrepTopPanel
          singleWorkspaceMode={this.props.singleWorkspaceMode}
          workspaceId={this.props.workspaceId}
          onSubmit={this.onSubmitToListener.bind(this)}
        />

        <div className="row dataprep-body">
          <div className="dataprep-main col-xs-9">
            <DataPrepTable />
            <DataPrepCLI />
          </div>

          <DataPrepSidePanel />
        </div>

        <DataPrepLoading />
      </div>
    );
  }
}
DataPrep.propTypes = {
  singleWorkspaceMode: PropTypes.bool,
  workspaceId: PropTypes.string,
  onSubmit: PropTypes.func
};
