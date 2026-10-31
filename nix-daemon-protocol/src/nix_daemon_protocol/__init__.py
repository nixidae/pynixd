"""Declarative binary serialization for Nix daemon protocol types."""

from .add_build_log import AddBuildLogRequest as AddBuildLogRequest
from .add_build_log import AddBuildLogResponse as AddBuildLogResponse
from .add_indirect_root import AddIndirectRootRequest as AddIndirectRootRequest
from .add_indirect_root import AddIndirectRootResponse as AddIndirectRootResponse
from .add_multiple_to_store import AddMultipleToStoreRequest as AddMultipleToStoreRequest
from .add_multiple_to_store import AddMultipleToStoreResponse as AddMultipleToStoreResponse
from .add_perm_root import AddPermRootRequest as AddPermRootRequest
from .add_perm_root import AddPermRootResponse as AddPermRootResponse
from .add_signatures import AddSignaturesRequest as AddSignaturesRequest
from .add_signatures import AddSignaturesResponse as AddSignaturesResponse
from .add_temp_root import AddTempRootRequest as AddTempRootRequest
from .add_temp_root import AddTempRootResponse as AddTempRootResponse
from .add_temp_roots import AddTempRootsRequest as AddTempRootsRequest
from .add_temp_roots import AddTempRootsResponse as AddTempRootsResponse
from .add_to_store import AddToStoreRequest as AddToStoreRequest
from .add_to_store import AddToStoreResponse as AddToStoreResponse
from .add_to_store_nar import AddToStoreNarRequest as AddToStoreNarRequest
from .add_to_store_nar import AddToStoreNarResponse as AddToStoreNarResponse
from .basic_derivation import BasicDerivation as BasicDerivation
from .build_derivation import BuildDerivationRequest as BuildDerivationRequest
from .build_derivation import BuildDerivationResponse as BuildDerivationResponse
from .build_paths import BuildPathsRequest as BuildPathsRequest
from .build_paths import BuildPathsResponse as BuildPathsResponse
from .build_paths_with_results import BuildPathsWithResultsRequest as BuildPathsWithResultsRequest
from .build_paths_with_results import BuildPathsWithResultsResponse as BuildPathsWithResultsResponse
from .build_result import (
    MAX_WIRE_STATUS as MAX_WIRE_STATUS,
)
from .build_result import BuildMode as BuildMode
from .build_result import BuildResult as BuildResult
from .build_result import (
    BuildResultStatus as BuildResultStatus,
)
from .build_result import BuiltOutput as BuiltOutput
from .collect_garbage import CollectGarbageRequest as CollectGarbageRequest
from .collect_garbage import CollectGarbageResponse as CollectGarbageResponse
from .collect_garbage import GCPaths as GCPaths
from .constants import FEATURE_ADD_TEMP_ROOTS as FEATURE_ADD_TEMP_ROOTS
from .constants import FEATURE_ADD_TO_STORE_SCANNING as FEATURE_ADD_TO_STORE_SCANNING
from .constants import FEATURE_DELETE_DEAD_SPECIFIC_REFERRERS as FEATURE_DELETE_DEAD_SPECIFIC_REFERRERS
from .constants import FEATURE_DISABLE_SET_OPTIONS as FEATURE_DISABLE_SET_OPTIONS
from .constants import FEATURE_EXCHANGE_PROTOCOL as FEATURE_EXCHANGE_PROTOCOL
from .constants import FEATURE_REALISATION_WITH_PATH as FEATURE_REALISATION_WITH_PATH
from .constants import FEATURE_SUBMIT_OUTPUT as FEATURE_SUBMIT_OUTPUT
from .constants import MINIMUM_REMOTE_PROTOCOL as MINIMUM_REMOTE_PROTOCOL
from .constants import PROTOCOL_VERSION as PROTOCOL_VERSION
from .constants import STANDARD_FEATURES as STANDARD_FEATURES
from .constants import STDERR_ERROR as STDERR_ERROR
from .constants import STDERR_LAST as STDERR_LAST
from .constants import STDERR_NEXT as STDERR_NEXT
from .constants import STDERR_RESULT as STDERR_RESULT
from .constants import STDERR_START_ACTIVITY as STDERR_START_ACTIVITY
from .constants import STDERR_STOP_ACTIVITY as STDERR_STOP_ACTIVITY
from .constants import SUPPORTED_PROTOCOL_VERSIONS as SUPPORTED_PROTOCOL_VERSIONS
from .constants import SUPPORTED_STANDARD_FEATURES as SUPPORTED_STANDARD_FEATURES
from .constants import WORKER_MAGIC_1 as WORKER_MAGIC_1
from .constants import WORKER_MAGIC_2 as WORKER_MAGIC_2
from .constants import is_supported_protocol as is_supported_protocol
from .constants import negotiate_features as negotiate_features
from .constants import proto as proto
from .constants import proto_str as proto_str
from .content_address import ContentAddress as ContentAddress
from .context import ReadContext as ReadContext
from .context import WriteContext as WriteContext
from .derivation_output import DerivationOutput as DerivationOutput
from .derivation_output import OutputKind as OutputKind
from .derived_path import DerivedPath as DerivedPath
from .drv_output import DrvOutput as DrvOutput
from .ensure_path import EnsurePathRequest as EnsurePathRequest
from .ensure_path import EnsurePathResponse as EnsurePathResponse
from .exceptions import UnsupportedProtocolVersion as UnsupportedProtocolVersion
from .find_roots import FindRootsEntry as FindRootsEntry
from .find_roots import FindRootsRequest as FindRootsRequest
from .find_roots import FindRootsResponse as FindRootsResponse
from .ids import LOCAL_STORE_ID as LOCAL_STORE_ID
from .ids import BuildId as BuildId
from .ids import RequestId as RequestId
from .ids import StoreId as StoreId
from .io import BytesReader as BytesReader
from .io import BytesWriter as BytesWriter
from .io import NixReader as NixReader
from .io import NixWriter as NixWriter
from .is_valid_path import IsValidPathRequest as IsValidPathRequest
from .is_valid_path import IsValidPathResponse as IsValidPathResponse
from .keyed_build_result import KeyedBuildResult as KeyedBuildResult
from .keyed_drv_output import KeyedDrvOutput as KeyedDrvOutput
from .logging import ProtocolLogger as ProtocolLogger
from .logs import ActivityField as ActivityField
from .logs import LogError as LogError
from .logs import LogNext as LogNext
from .logs import LogResult as LogResult
from .logs import LogStartActivity as LogStartActivity
from .logs import LogStopActivity as LogStopActivity
from .logs import TraceLine as TraceLine
from .logs import WireLogs as WireLogs
from .nar_from_path import NarFromPathRequest as NarFromPathRequest
from .nar_from_path import NarFromPathResponse as NarFromPathResponse
from .nar_hash import NARHash as NARHash
from .operations import STANDARD_OPERATIONS as STANDARD_OPERATIONS
from .operations import StandardOperation as StandardOperation
from .opt_microseconds import OptMicroseconds as OptMicroseconds
from .optimise_store import OptimiseStoreRequest as OptimiseStoreRequest
from .optimise_store import OptimiseStoreResponse as OptimiseStoreResponse
from .path_info import UnkeyedValidPathInfo as UnkeyedValidPathInfo
from .protocol import ActivityType as ActivityType
from .protocol import FieldType as FieldType
from .protocol import FileIngestionMethod as FileIngestionMethod
from .protocol import GCAction as GCAction
from .protocol import OptTrusted as OptTrusted
from .protocol import ResultType as ResultType
from .protocol import Verbosity as Verbosity
from .query_all_valid_paths import QueryAllValidPathsRequest as QueryAllValidPathsRequest
from .query_all_valid_paths import QueryAllValidPathsResponse as QueryAllValidPathsResponse
from .query_derivation_output_map import QueryDerivationOutputMapRequest as QueryDerivationOutputMapRequest
from .query_derivation_output_map import QueryDerivationOutputMapResponse as QueryDerivationOutputMapResponse
from .query_missing import QueryMissingRequest as QueryMissingRequest
from .query_missing import QueryMissingResponse as QueryMissingResponse
from .query_path_from_hash_part import QueryPathFromHashPartRequest as QueryPathFromHashPartRequest
from .query_path_from_hash_part import QueryPathFromHashPartResponse as QueryPathFromHashPartResponse
from .query_path_info import QueryPathInfoRequest as QueryPathInfoRequest
from .query_path_info import QueryPathInfoResponse as QueryPathInfoResponse
from .query_realisation import QueryRealisationRequest as QueryRealisationRequest
from .query_realisation import QueryRealisationResponse as QueryRealisationResponse
from .query_referrers import QueryReferrersRequest as QueryReferrersRequest
from .query_referrers import QueryReferrersResponse as QueryReferrersResponse
from .query_substitutable_path_info import (
    QuerySubstitutablePathInfoRequest as QuerySubstitutablePathInfoRequest,
)
from .query_substitutable_path_info import (
    QuerySubstitutablePathInfoResponse as QuerySubstitutablePathInfoResponse,
)
from .query_substitutable_path_info import (
    UnkeyedSubstitutablePathInfo as UnkeyedSubstitutablePathInfo,
)
from .query_substitutable_path_infos import (
    QuerySubstitutablePathInfosRequest as QuerySubstitutablePathInfosRequest,
)
from .query_substitutable_path_infos import (
    QuerySubstitutablePathInfosResponse as QuerySubstitutablePathInfosResponse,
)
from .query_substitutable_path_infos import (
    SubstitutablePathInfo as SubstitutablePathInfo,
)
from .query_substitutable_paths import QuerySubstitutablePathsRequest as QuerySubstitutablePathsRequest
from .query_substitutable_paths import QuerySubstitutablePathsResponse as QuerySubstitutablePathsResponse
from .query_valid_derivers import QueryValidDeriversRequest as QueryValidDeriversRequest
from .query_valid_derivers import QueryValidDeriversResponse as QueryValidDeriversResponse
from .query_valid_paths import QueryValidPathsRequest as QueryValidPathsRequest
from .query_valid_paths import QueryValidPathsResponse as QueryValidPathsResponse
from .realisation import Realisation as Realisation
from .register_drv_output import RegisterDrvOutputRequest as RegisterDrvOutputRequest
from .register_drv_output import RegisterDrvOutputResponse as RegisterDrvOutputResponse
from .set_options import SetOptionsRequest as SetOptionsRequest
from .set_options import SetOptionsResponse as SetOptionsResponse
from .signature import Signature as Signature
from .store_dir import (
    DEFAULT_STORE_DIR as DEFAULT_STORE_DIR,
)
from .store_dir import (
    in_store_dir as in_store_dir,
)
from .store_dir import (
    on_disk as on_disk,
)
from .store_dir import (
    real_store_dir as real_store_dir,
)
from .store_dir import (
    reset_store_dir as reset_store_dir,
)
from .store_dir import (
    set_real_store_dir as set_real_store_dir,
)
from .store_dir import (
    set_store_dir as set_store_dir,
)
from .store_dir import (
    store_dir as store_dir,
)
from .store_dir import (
    store_prefix as store_prefix,
)
from .store_path import StorePath as StorePath
from .unkeyed_realisation import UnkeyedRealisation as UnkeyedRealisation
from .valid_path_info import ValidPathInfo as ValidPathInfo
from .verify_store import VerifyStoreRequest as VerifyStoreRequest
from .verify_store import VerifyStoreResponse as VerifyStoreResponse
from .wire_integer import WireUInt64 as WireUInt64
from .wire_message import VersionMeta as VersionMeta
from .wire_message import WireField as WireField
from .wire_message import WireModel as WireModel
from .wire_ops import WIRE_REGISTRY as WIRE_REGISTRY
from .wire_ops import WireRequest as WireRequest
from .wire_ops import WireResponse as WireResponse
from .wire_scalar import WireScalar as WireScalar
from .wire_string import WireString as WireString
from .wire_time import Time as Time
from .wire_time import TimeSpan as TimeSpan
