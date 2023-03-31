import json
from importlib import import_module
from json import JSONDecodeError
from typing import Any, Dict

import foundry
import requests
from foundry.helpers import to_snake_case


def check_for_errors(response: requests.Response, returns_json=True):
    try:
        response_json = response.json()
    except JSONDecodeError:
        if returns_json:
            raise ValueError("Invalid response - cannot convert to json")
        else:
            return

    if "errorName" in response_json:
        error_name = response_json["errorName"]
        try:
            error_module = import_module(get_error_module(error_name))
            del response_json["errorName"]
            if "parameters" in response_json.keys() and not response_json.get("parameters"):
                del response_json["parameters"]
            raise getattr(error_module, error_name)(response_json)
        except ModuleNotFoundError:
            raise foundry.errors.PalantirRPCException(response_json) from None


def get_error_module(error_name: str):
    if error_name == "FolderNotFound":
        return "foundry.errors._folder_not_found"
    if error_name == "InvalidPageToken":
        return "foundry.errors._invalid_page_token"
    if error_name == "ApiFeaturePreviewUsageOnly":
        return "foundry.errors._api_feature_preview_usage_only"
    if error_name == "InvalidParameterCombination":
        return "foundry.errors._invalid_parameter_combination"
    if error_name == "ApiUsageDenied":
        return "foundry.errors._api_usage_denied"
    if error_name == "InvalidPageSize":
        return "foundry.errors._invalid_page_size"
    if error_name == "FileNotFoundOnTransactionRange":
        return "foundry.errors._file_not_found_on_transaction_range"
    if error_name == "ResourceNameAlreadyExists":
        return "foundry.errors._resource_name_already_exists"
    if error_name == "ReadTablePermissionDenied":
        return "foundry.datasets.errors._read_table_permission_denied"
    if error_name == "OpenTransactionAlreadyExists":
        return "foundry.datasets.errors._open_transaction_already_exists"
    if error_name == "CreateTransactionPermissionDenied":
        return "foundry.datasets.errors._create_transaction_permission_denied"
    if error_name == "BranchAlreadyExists":
        return "foundry.datasets.errors._branch_already_exists"
    if error_name == "CreateBranchPermissionDenied":
        return "foundry.datasets.errors._create_branch_permission_denied"
    if error_name == "AbortTransactionPermissionDenied":
        return "foundry.datasets.errors._abort_transaction_permission_denied"
    if error_name == "CreateDatasetPermissionDenied":
        return "foundry.datasets.errors._create_dataset_permission_denied"
    if error_name == "FileAlreadyExists":
        return "foundry.datasets.errors._file_already_exists"
    if error_name == "FileNotFoundOnBranch":
        return "foundry.datasets.errors._file_not_found_on_branch"
    if error_name == "TransactionNotCommitted":
        return "foundry.datasets.errors._transaction_not_committed"
    if error_name == "DatasetNotFound":
        return "foundry.datasets.errors._dataset_not_found"
    if error_name == "TransactionNotOpen":
        return "foundry.datasets.errors._transaction_not_open"
    if error_name == "ColumnTypesNotSupported":
        return "foundry.datasets.errors._column_types_not_supported"
    if error_name == "CommitTransactionPermissionDenied":
        return "foundry.datasets.errors._commit_transaction_permission_denied"
    if error_name == "TransactionNotFound":
        return "foundry.datasets.errors._transaction_not_found"
    if error_name == "InvalidBranchId":
        return "foundry.datasets.errors._invalid_branch_id"
    if error_name == "UploadFilePermissionDenied":
        return "foundry.datasets.errors._upload_file_permission_denied"
    if error_name == "BranchNotFound":
        return "foundry.datasets.errors._branch_not_found"
    if error_name == "InvalidTransactionType":
        return "foundry.datasets.errors._invalid_transaction_type"
    if error_name == "DeleteBranchPermissionDenied":
        return "foundry.datasets.errors._delete_branch_permission_denied"
    raise ModuleNotFoundError(f"Error name {error_name} not recognized, falling back to PalantirRPCException")


def format_error_message(fields: Dict[str, Any]) -> str:
    return json.dumps(fields, sort_keys=True, indent=4)
