import {gql, useQuery} from '@apollo/client';
import {Box, ButtonLink, Colors} from '@dagster-io/ui';
import * as React from 'react';
import {useHistory} from 'react-router-dom';
import styled from 'styled-components/macro';

import {SharedToaster} from '../app/DomUtils';
import {useQueryRefreshAtInterval} from '../app/QueryRefresh';
import {StatusAndMessage} from '../instance/DeploymentStatusType';
import {RepositoryLocationLoadStatus} from '../types/globalTypes';
import {WorkspaceContext} from '../workspace/WorkspaceContext';

import {
  CodeLocationStatusQuery,
  CodeLocationStatusQuery_locationStatusesOrError_WorkspaceLocationStatusEntries_entries,
} from './types/CodeLocationStatusQuery';

type LocationStatusEntry = CodeLocationStatusQuery_locationStatusesOrError_WorkspaceLocationStatusEntries_entries;

const POLL_INTERVAL = 5 * 1000;

export const useCodeLocationsStatus = (skip = false): StatusAndMessage | null => {
  const {locationEntries, refetch} = React.useContext(WorkspaceContext);
  const history = useHistory();

  const [showSpinner, setShowSpinner] = React.useState(false);

  const queryData = useQuery<CodeLocationStatusQuery>(CODE_LOCATION_STATUS_QUERY, {
    fetchPolicy: 'network-only',
    notifyOnNetworkStatusChange: true,
    skip,
  });

  useQueryRefreshAtInterval(queryData, POLL_INTERVAL);

  const {data, previousData} = queryData;

  const onClickViewButton = React.useCallback(() => {
    history.push('/locations');
  }, [history]);

  // Reload the workspace, but don't toast about it.
  const reloadWorkspaceQuietly = React.useCallback(async () => {
    setShowSpinner(true);
    await refetch();
    setShowSpinner(false);
  }, [refetch]);

  // Reload the workspace, and show a success or error toast upon completion.
  const reloadWorkspaceLoudly = React.useCallback(async () => {
    setShowSpinner(true);
    const result = await refetch();
    setShowSpinner(false);

    const anyErrors =
      result.data.workspaceOrError.__typename === 'PythonError' ||
      result.data.workspaceOrError.locationEntries.some(
        (entry) => entry.locationOrLoadError?.__typename === 'PythonError',
      );

    const showViewButton = !alreadyViewingCodeLocations();

    if (anyErrors) {
      SharedToaster.show({
        intent: 'warning',
        message: (
          <Box flex={{direction: 'row', justifyContent: 'space-between', gap: 24, grow: 1}}>
            <div>Definitions loaded with errors</div>
            {showViewButton ? <ViewCodeLocationsButton onClick={onClickViewButton} /> : null}
          </Box>
        ),
        icon: 'check_circle',
      });
    } else {
      SharedToaster.show({
        intent: 'success',
        message: (
          <Box flex={{direction: 'row', justifyContent: 'space-between', gap: 24, grow: 1}}>
            <div>Definitions reloaded</div>
            {showViewButton ? <ViewCodeLocationsButton onClick={onClickViewButton} /> : null}
          </Box>
        ),
        icon: 'check_circle',
      });
    }
  }, [onClickViewButton, refetch]);

  // Given the previous and current code locations, determine whether to show a) a loading spinner
  // and/or b) a toast indicating that a code location is being reloaded.
  React.useEffect(() => {
    const previousEntries =
      previousData?.locationStatusesOrError?.__typename === 'WorkspaceLocationStatusEntries'
        ? previousData?.locationStatusesOrError.entries
        : [];
    const currentEntries =
      data?.locationStatusesOrError?.__typename === 'WorkspaceLocationStatusEntries'
        ? data?.locationStatusesOrError.entries
        : [];

    const _build_entries_by_name = (
      entries: LocationStatusEntry[],
    ): {[key: string]: LocationStatusEntry} => {
      const entries_by_name = {};
      entries.forEach((entry) => {
        entries_by_name[entry.id] = entry;
      });
      return entries_by_name;
    };
    const previousEntriesByName = _build_entries_by_name(previousEntries);
    const currentEntriesByName = _build_entries_by_name(currentEntries);
    const hasUpdatedEntries =
      currentEntries.length &&
      currentEntries.some(
        (entry) =>
          !(entry.id in previousEntriesByName) ||
          previousEntriesByName[entry.id].updateTimestamp <
            currentEntriesByName[entry.id].updateTimestamp,
      );

    // At least one code location has been removed. Reload, but don't make a big deal about it
    // since this was probably done manually.
    if (previousEntries.length > currentEntries.length && !hasUpdatedEntries) {
      reloadWorkspaceQuietly();
      return;
    }

    const currentlyLoading = currentEntries.filter(
      ({loadStatus}) => loadStatus === RepositoryLocationLoadStatus.LOADING,
    );
    const anyCurrentlyLoading = currentlyLoading.length > 0;

    // If this is a fresh pageload and any locations are loading, show the spinner but not the toaster.
    if (!previousData) {
      if (anyCurrentlyLoading) {
        setShowSpinner(true);
      }
      return;
    }

    const showViewButton = !alreadyViewingCodeLocations();

    // We have a new entry, and it has already finished loading. Wow! It's surprisingly fast for it
    // to have finished loading so quickly, but go ahead and indicate that the location has
    // been added, then reload the workspace.
    if (currentEntries.length > previousEntries.length && !currentlyLoading.length) {
      const previousMap: {[id: string]: true} = previousEntries.reduce(
        (accum, {id}) => ({...accum, [id]: true}),
        {},
      );

      // Count the number of new code locations.
      const addedEntries: string[] = [];
      currentEntries.forEach(({id}) => {
        if (!previousMap.hasOwnProperty(id)) {
          addedEntries.push(id);
        }
      });

      SharedToaster.show({
        intent: 'primary',
        message: (
          <Box flex={{direction: 'row', justifyContent: 'space-between', gap: 24, grow: 1}}>
            {addedEntries.length === 1 ? (
              <span>
                Code location <strong>{addedEntries[0]}</strong> added
              </span>
            ) : (
              <span>{addedEntries.length} code locations added</span>
            )}
            {showViewButton ? <ViewCodeLocationsButton onClick={onClickViewButton} /> : null}
          </Box>
        ),
        icon: 'add_circle',
      });

      reloadWorkspaceLoudly();
      return;
    }

    const anyPreviouslyLoading = previousEntries.some(
      ({loadStatus}) => loadStatus === RepositoryLocationLoadStatus.LOADING,
    );

    // One or more code locations are updating, so let the user know. We will not refetch the workspace
    // until all code locations are done updating.
    if (!anyPreviouslyLoading && anyCurrentlyLoading) {
      setShowSpinner(true);

      SharedToaster.show({
        intent: 'primary',
        message: (
          <Box flex={{direction: 'row', justifyContent: 'space-between', gap: 24, grow: 1}}>
            {currentlyLoading.length === 1 ? (
              <span>
                Updating <strong>{currentlyLoading[0].name}</strong>
              </span>
            ) : (
              <span>Updating {currentlyLoading.length} code locations</span>
            )}
            {showViewButton ? <ViewCodeLocationsButton onClick={onClickViewButton} /> : null}
          </Box>
        ),
        icon: 'refresh',
      });

      return;
    }

    // A location was previously loading, and no longer is. Our workspace is ready. Refetch it.
    if (anyPreviouslyLoading && !anyCurrentlyLoading) {
      reloadWorkspaceLoudly();
      return;
    }

    if (hasUpdatedEntries) {
      reloadWorkspaceLoudly();
      return;
    }

    // It's unlikely that we've made it to this point, since being inside this effect should
    // indicate that `data` and `previousData` have differences that would have been handled by
    // the conditionals above.
  }, [data, previousData, reloadWorkspaceQuietly, reloadWorkspaceLoudly, onClickViewButton]);

  if (showSpinner) {
    return {
      type: 'spinner',
      content: <div>Loading definitions…</div>,
    };
  }

  const repoErrors = locationEntries.filter(
    (locationEntry) => locationEntry.locationOrLoadError?.__typename === 'PythonError',
  );

  if (repoErrors.length) {
    return {
      type: 'warning',
      content: (
        <div style={{whiteSpace: 'nowrap'}}>{`${repoErrors.length} ${
          repoErrors.length === 1 ? 'code location failed to load' : 'code locations failed to load'
        }`}</div>
      ),
    };
  }

  return null;
};

const alreadyViewingCodeLocations = () => document.location.pathname.endsWith('/locations');

const ViewCodeLocationsButton: React.FC<{onClick: () => void}> = ({onClick}) => {
  return (
    <ViewButton onClick={onClick} color={Colors.White}>
      View
    </ViewButton>
  );
};

const ViewButton = styled(ButtonLink)`
  white-space: nowrap;
`;

const CODE_LOCATION_STATUS_QUERY = gql`
  query CodeLocationStatusQuery {
    locationStatusesOrError {
      __typename
      ... on WorkspaceLocationStatusEntries {
        entries {
          id
          name
          loadStatus
          updateTimestamp
        }
      }
    }
  }
`;
