//! Startup settings and module identity.
//! The serving path and config checks share the same settings loader.

use std::path::{Path, PathBuf};

use subc_protocol::SUBC_MODULE_ID_ENV;

use crate::{config_overrides, ModuleError, DEFAULT_MODULE_ID};

pub(super) fn check_config_at(path: &Path) -> Result<(), ModuleError> {
    config_overrides::initialize_config(path).map_err(ModuleError::Message)
}

pub(super) struct ModuleConfig {
    pub(super) connection_file_path: PathBuf,
    pub(super) module_id: String,
    pub(super) managed_settings: bool,
}

impl ModuleConfig {
    pub(super) fn from_env(
        connection_file_path: PathBuf,
        managed_settings: bool,
    ) -> Result<Self, ModuleError> {
        let module_id = std::env::var(SUBC_MODULE_ID_ENV)
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_MODULE_ID.to_string());
        Ok(Self {
            connection_file_path,
            module_id,
            managed_settings,
        })
    }
}
