import {
  Box,
  Button,
  ButtonLink,
  Colors,
  Icon,
  JoinedButtons,
  MiddleTruncate,
  Tag,
  Tooltip,
} from '@dagster-io/ui';
import * as React from 'react';
import {Link} from 'react-router-dom';

import {usePermissions} from '../app/Permissions';
import {ReloadRepositoryLocationButton} from '../nav/ReloadRepositoryLocationButton';
import {
  buildReloadFnForLocation,
  useRepositoryLocationReload,
} from '../nav/useRepositoryLocationReload';
import {TimeFromNow} from '../ui/TimeFromNow';

import {CodeLocationMenu} from './CodeLocationMenu';
import {RepositoryCountTags} from './RepositoryCountTags';
import {RepositoryLocationNonBlockingErrorDialog} from './RepositoryLocationErrorDialog';
import {WorkspaceRepositoryLocationNode} from './WorkspaceContext';
import {buildRepoAddress} from './buildRepoAddress';
import {repoAddressAsHumanString} from './repoAddressAsString';
import {workspacePathFromAddress} from './workspacePath';

interface Props {
  locationNode: WorkspaceRepositoryLocationNode;
}

export const CodeLocationRowSet: React.FC<Props> = ({locationNode}) => {
  const {name, locationOrLoadError} = locationNode;

  if (!locationOrLoadError || locationOrLoadError?.__typename === 'PythonError') {
    return (
      <tr>
        <td style={{maxWidth: '400px', color: Colors.Gray500}}>
          <MiddleTruncate text={name} />
        </td>
        <td>
          <LocationStatus location={name} locationOrError={locationNode} />
        </td>
        <td style={{whiteSpace: 'nowrap'}}>
          <TimeFromNow unixTimestamp={locationNode.updatedTimestamp} />
        </td>
        <td>{'\u2013'}</td>
        <td style={{width: '180px'}}>
          <JoinedButtons>
            <ReloadButton location={name} />
            <CodeLocationMenu locationNode={locationNode} />
          </JoinedButtons>
        </td>
      </tr>
    );
  }

  const repositories = [...locationOrLoadError.repositories].sort((a, b) =>
    a.name.localeCompare(b.name),
  );

  return (
    <>
      {repositories.map((repository) => {
        const repoAddress = buildRepoAddress(repository.name, name);
        return (
          <tr key={repoAddressAsHumanString(repoAddress)}>
            <td style={{maxWidth: '400px', fontWeight: 500}}>
              <Link to={workspacePathFromAddress(repoAddress)}>
                <MiddleTruncate text={repoAddressAsHumanString(repoAddress)} />
              </Link>
            </td>
            <td>
              <LocationStatus location={repository.name} locationOrError={locationNode} />
            </td>
            <td style={{whiteSpace: 'nowrap'}}>
              <TimeFromNow unixTimestamp={locationNode.updatedTimestamp} />
            </td>
            <td>
              <RepositoryCountTags repo={repository} repoAddress={repoAddress} />
            </td>
            <td style={{width: '180px'}}>
              <JoinedButtons>
                <ReloadButton location={name} />
                <CodeLocationMenu locationNode={locationNode} />
              </JoinedButtons>
            </td>
          </tr>
        );
      })}
    </>
  );
};

const LocationStatus: React.FC<{
  location: string;
  locationOrError: WorkspaceRepositoryLocationNode;
}> = (props) => {
  const {location, locationOrError} = props;
  const [showDialog, setShowDialog] = React.useState(false);

  const reloadFn = React.useMemo(() => buildReloadFnForLocation(location), [location]);
  const {reloading, tryReload} = useRepositoryLocationReload({
    scope: 'location',
    reloadFn,
  });

  if (locationOrError.loadStatus === 'LOADING') {
    if (locationOrError.locationOrLoadError) {
      return (
        <Tag minimal intent="primary">
          Updating...
        </Tag>
      );
    } else {
      return (
        <Tag minimal intent="primary">
          Loading...
        </Tag>
      );
    }
  }

  if (locationOrError.locationOrLoadError?.__typename === 'PythonError') {
    return (
      <>
        <Box flex={{alignItems: 'center', gap: 12}}>
          <Tag minimal intent="danger">
            Failed
          </Tag>
          <ButtonLink onClick={() => setShowDialog(true)}>
            <span style={{fontSize: '14px'}}>View error</span>
          </ButtonLink>
        </Box>
        <RepositoryLocationNonBlockingErrorDialog
          location={location}
          isOpen={showDialog}
          error={locationOrError.locationOrLoadError}
          reloading={reloading}
          onDismiss={() => setShowDialog(false)}
          onTryReload={() => tryReload()}
        />
      </>
    );
  }

  return (
    <Tag minimal intent="success">
      Loaded
    </Tag>
  );
};

const ReloadButton: React.FC<{
  location: string;
}> = (props) => {
  const {location} = props;
  const {canReloadRepositoryLocation} = usePermissions();

  if (!canReloadRepositoryLocation.enabled) {
    return (
      <Tooltip content={canReloadRepositoryLocation.disabledReason} useDisabledButtonTooltipFix>
        <Button icon={<Icon name="refresh" />} disabled>
          Reload
        </Button>
      </Tooltip>
    );
  }

  return (
    <ReloadRepositoryLocationButton location={location}>
      {({reloading, tryReload}) => (
        <Box flex={{direction: 'row', alignItems: 'center', gap: 4}}>
          <Button icon={<Icon name="refresh" />} loading={reloading} onClick={() => tryReload()}>
            Reload
          </Button>
        </Box>
      )}
    </ReloadRepositoryLocationButton>
  );
};
