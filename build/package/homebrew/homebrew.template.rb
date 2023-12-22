class KeboolaCli < Formula
  desc "Keboola CLI tool"
  homepage "https://www.keboola.com/product/cli"
  license "Apache2"
  version "${VERSION}"

  if OS.mac? && Hardware::CPU.arm?
    url "https://cli-dist.keboola.com/zip/keboola-cli_${VERSION}_darwin_arm64.zip"
    sha256 "${DARWIN_ARM_TARGET_SHA256}"
  end
  if OS.mac? && Hardware::CPU.intel?
    url "https://cli-dist.keboola.com/zip/keboola-cli_${VERSION}_darwin_amd64.zip"
    sha256 "${DARWIN_AMD_TARGET_SHA256}"
  end
  if OS.linux? && Hardware::CPU.intel?
    url "https://cli-dist.keboola.com/zip/keboola-cli_${VERSION}_linux_amd64.zip"
    sha256 "${LINUX_AMD_TARGET_SHA256}"
  end
  if OS.linux? && Hardware::CPU.arm? && !Hardware::CPU.is_64_bit?
    url "https://cli-dist.keboola.com/zip/keboola-cli_${VERSION}_linux_armv6.zip"
    sha256 "${LINUX_ARM_TARGET_SHA256}"
  end
  if OS.linux? && Hardware::CPU.arm? && Hardware::CPU.is_64_bit?
    url "https://cli-dist.keboola.com/zip/keboola-cli_${VERSION}_linux_arm64.zip"
    sha256 "${LINUX_ARM64_TARGET_SHA256}"
  end

  depends_on "git" => :recommended

  def install
    bin.install "kbc"
    bin.install_symlink Dir["#{libexec}/bin/*"]
  end

  test do
    system "#{bin}/kbc --version"
  end
end
