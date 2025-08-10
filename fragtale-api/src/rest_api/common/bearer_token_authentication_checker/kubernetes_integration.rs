/*
    Copyright 2025 MydriaTech AB

    Licensed under the Apache License 2.0 with Free world makers exception
    1.0.0 (the "License"); you may not use this file except in compliance with
    the License. You should have obtained a copy of the License with the source
    or binary distribution in file named

        LICENSE-Apache-2.0-with-FWM-Exception-1.0.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

//! Kubernetes integration utils.

use tokio::fs::File;
use tokio::io::AsyncReadExt;

/// Kubernetes integration utils.
pub struct KubernetesIntegration {}

impl KubernetesIntegration {
    /// Kubernetes API server host.
    const ENV_K8S_HOST: &str = "KUBERNETES_SERVICE_HOST";
    /// Kubernetes API server HTTPS port.
    const ENV_K8S_PORT: &str = "KUBERNETES_SERVICE_PORT_HTTPS";

    /// Kubernetes bearer token for service account for K8s API.
    const FILE_K8S_SA_TOKEN: &str = "/var/run/secrets/kubernetes.io/serviceaccount/token";
    /// Kubernetes projected volume bearer token for service account.
    const FILE_K8S_SA_TOKEN_PROJECTED: &str = "/var/run/secrets/tokens/service-account";
    /// Kubernetes API server HTTPS trunt anchor.
    const FILE_K8S_TRUST_ANCHORS_PEM: &str = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt";

    /// Returns the URL `https://${KUBERNETES_SERVICE_HOST}:${KUBERNETES_SERVICE_PORT_HTTPS}/.well-known/openid-configuration`
    pub fn build_openid_config_url() -> Result<String, std::env::VarError> {
        // Get KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT_HTTPS
        let host = std::env::var(Self::ENV_K8S_HOST)?;
        let port = std::env::var(Self::ENV_K8S_PORT)?;
        Ok(format!(
            "https://{host}:{port}/.well-known/openid-configuration"
        ))
    }

    /// Read Kubernetes bearer token for service account to K8s API.
    pub async fn read_service_account_token() -> Result<String, Box<dyn core::error::Error>> {
        Self::read_file(Self::FILE_K8S_SA_TOKEN).await
    }

    /// Read Kubernetes projected volume bearer token for service account.
    pub async fn read_service_account_token_projected()
    -> Result<String, Box<dyn core::error::Error>> {
        Self::read_file(Self::FILE_K8S_SA_TOKEN_PROJECTED).await
    }

    /// Read trust-anchors from
    /// '/var/run/secrets/kubernetes.io/serviceaccount/ca.crt'.
    pub async fn read_trust_anchors_pem() -> Result<String, Box<dyn core::error::Error>> {
        Self::read_file(Self::FILE_K8S_TRUST_ANCHORS_PEM).await
    }

    /// Read full text file.
    async fn read_file(filename: &str) -> Result<String, Box<dyn core::error::Error>> {
        let mut file = File::open(filename).await?;
        let mut contents = vec![];
        file.read_to_end(&mut contents).await?;
        Ok(std::str::from_utf8(&contents)?.to_string())
    }
}
