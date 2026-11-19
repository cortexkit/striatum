//! Startup settings and module identity.
//! The serving path and config checks share the same settings loader.

use std::path::{Path, PathBuf};

use subc_protocol::SUBC_MODULE_ID_ENV;

use crate::{config_overrides, ModuleError, DEFAULT_MODULE_ID};

pub(super) fn check_config_at(path: &Path) -> Result<(), ModuleError> {
    config_overrides::initialize_config(path).map_err(ModuleError::Message)
}

pub(super) fn module_id_from_env() -> String {
    std::env::var(SUBC_MODULE_ID_ENV)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_MODULE_ID.to_string())
}

pub(super) struct ModuleConfig {
    pub(super) connection_file_path: PathBuf,
    pub(super) module_id: String,
    pub(super) config_path: PathBuf,
}

impl ModuleConfig {
    pub(super) fn from_env(connection_file_path: PathBuf) -> Result<Self, ModuleError> {
        let module_id = module_id_from_env();
        let config_path =
            config_overrides::config_path(&module_id).map_err(ModuleError::Message)?;
        Ok(Self {
            connection_file_path,
            module_id,
            config_path,
        })
    }
}
