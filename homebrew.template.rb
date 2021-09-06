class KeboolaAsCode < Formula
  desc "Keboola as Code cli tool"
  homepage "https://github.com/keboola/keboola-as-code"
  license "MIT"
  bottle :unneeded

  if OS.Mac? && Hardware::CPU.arm?
    url "https://github.com/keboola/keboola-as-code/releases/download/v${TARGET_VERSION}/kbc_${TARGET_VERSION}_darwin_arm64.zip"
    sha256 "${ARM_TARGET_SHA256}"
  end
  if OS.Mac? && Hardware::CPU.intel?
        url "https://github.com/keboola/keboola-as-code/releases/download/v${TARGET_VERSION}/kbc_${TARGET_VERSION}_darwin_amd64.zip"
    sha256 "${AMD_TARGET_SHA256}"
  end

  def install
    bin.install "kbc"
    bin.install_symlink Dir["#{libexec}/bin/*"]
  end

end
