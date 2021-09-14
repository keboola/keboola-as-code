class KeboolaAsCode < Formula
  desc "Keboola as Code cli tool"
  homepage "https://github.com/keboola/keboola-as-code"
  license "MIT"
  bottle :unneeded

  if OS.mac? && Hardware::CPU.arm?
    url "https://github.com/keboola/keboola-as-code/releases/download/${TAG}/kbc_${VERSION}_darwin_arm64.zip"
    sha256 "${DARWIN_ARM_TARGET_SHA256}"
  end
  if OS.mac? && Hardware::CPU.intel?
    url "https://github.com/keboola/keboola-as-code/releases/download/${TAG}/kbc_${VERSION}_darwin_amd64.zip"
    sha256 "${DARWIN_AMD_TARGET_SHA256}"
  end
  if OS.linux? && Hardware::CPU.intel?
    url "https://github.com/keboola/keboola-as-code/releases/download/${TAG}/kbc_${VERSION}_linux_amd64.zip"
    sha256 "${LINUX_AMD_TARGET_SHA256}"
  end
  if OS.linux? && Hardware::CPU.arm? && !Hardware::CPU.is_64_bit?
    url "https://github.com/keboola/keboola-as-code/releases/download/${TAG}/kbc_${VERSION}_linux_armv6.zip"
    sha256 "${LINUX_ARM_TARGET_SHA256}"
  end
  if OS.linux? && Hardware::CPU.arm? && Hardware::CPU.is_64_bit?
    url "https://github.com/keboola/keboola-as-code/releases/download/${TAG}/kbc_${VERSION}_linux_arm64.zip"
    sha256 "${LINUX_ARM64_TARGET_SHA256}"
  end

  def install
    bin.install "kbc"
    bin.install_symlink Dir["#{libexec}/bin/*"]
  end

end
