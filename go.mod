module github.com/keboola/keboola-as-code

go 1.24.2

replace github.com/google/go-jsonnet => github.com/keboola/go-jsonnet v0.20.1-0.20240430105602-71646d8d4fa9

replace github.com/oauth2-proxy/oauth2-proxy/v7 => github.com/keboola/go-oauth2-proxy/v7 v7.6.1-0.20250325124709-f4d482276c37

replace github.com/oauth2-proxy/mockoidc => github.com/keboola/go-mockoidc v0.0.0-20240405064136-5229d2b53db6

require (
	ariga.io/atlas v0.32.1
	entgo.io/ent v0.14.4
	github.com/ActiveState/vt10x v1.3.1
	github.com/AlecAivazis/survey/v2 v2.3.7
	github.com/Masterminds/semver v1.5.0
	github.com/MichaelMure/go-term-markdown v0.1.4
	github.com/Netflix/go-expect v0.0.0-20220104043353-73e0943537d2
	github.com/Shopify/toxiproxy/v2 v2.12.0
	github.com/acarl005/stripansi v0.0.0-20180116102854-5a71ef0e047d
	github.com/armon/go-radix v1.0.1-0.20221118154546-54df44f2176c
	github.com/c2h5oh/datasize v0.0.0-20231215233829-aa82cc1e6500
	github.com/ccoveille/go-safecast v1.6.1
	github.com/cenkalti/backoff/v4 v4.3.0
	github.com/coder/websocket v1.8.13
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc
	github.com/dgraph-io/ristretto/v2 v2.2.0
	github.com/fatih/color v1.18.0
	github.com/go-playground/locales v0.14.1
	github.com/go-playground/universal-translator v0.18.1
	github.com/go-playground/validator/v10 v10.26.0
	github.com/go-resty/resty/v2 v2.16.5
	github.com/gofrs/flock v0.12.1
	github.com/gofrs/uuid/v5 v5.3.2
	github.com/google/go-cmp v0.7.0
	github.com/google/go-jsonnet v0.20.0
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510
	github.com/hashicorp/yamux v0.1.2
	github.com/jarcoal/httpmock v1.4.0
	github.com/joho/godotenv v1.5.1
	github.com/jonboulle/clockwork v0.5.0
	github.com/jpillora/longestcommon v0.0.0-20161227235612-adb9d91ee629
	github.com/json-iterator/go v1.1.12
	github.com/keboola/go-client v1.31.0
	github.com/keboola/go-cloud-encrypt v0.0.0-20250422071622-41a5d5547c43
	github.com/keboola/go-utils v1.3.3
	github.com/klauspost/compress v1.18.0
	github.com/klauspost/pgzip v1.2.6
	github.com/kylelemons/godebug v1.1.0
	github.com/lafikl/consistent v0.0.0-20220512074542-bdd3606bfc3e
	github.com/lestrrat-go/strftime v1.1.0
	github.com/mattn/go-sqlite3 v1.14.28
	github.com/miekg/dns v1.1.65
	github.com/mitchellh/hashstructure/v2 v2.0.2
	github.com/oauth2-proxy/mockoidc v0.0.0-20240214162133-caebfff84d25
	github.com/oauth2-proxy/oauth2-proxy/v7 v7.8.2
	github.com/pquerna/cachecontrol v0.2.0
	github.com/prometheus/client_golang v1.22.0
	github.com/qiangxue/fasthttp-routing v0.0.0-20160225050629-6ccdc2a18d87
	github.com/relvacode/iso8601 v1.6.0
	github.com/rs/zerolog v1.34.0
	github.com/santhosh-tekuri/jsonschema/v5 v5.3.1
	github.com/schollz/progressbar/v3 v3.18.0
	github.com/sirupsen/logrus v1.9.3
	github.com/spf13/afero v1.14.0
	github.com/spf13/cast v1.8.0
	github.com/spf13/cobra v1.9.1
	github.com/spf13/pflag v1.0.6
	github.com/spf13/viper v1.20.1
	github.com/stretchr/testify v1.10.0
	github.com/umisama/go-regexpcache v0.0.0-20150417035358-2444a542492f
	github.com/urfave/negroni v1.0.0
	github.com/valyala/fasthttp v1.61.0
	github.com/valyala/fastjson v1.6.4
	github.com/writeas/go-strip-markdown v2.0.1+incompatible
	github.com/xtaci/kcp-go/v5 v5.6.20
	go.etcd.io/etcd/api/v3 v3.5.21
	go.etcd.io/etcd/client/v3 v3.5.21
	go.etcd.io/etcd/tests/v3 v3.5.21
	go.nhat.io/aferocopy/v2 v2.0.3
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.60.0
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.60.0
	go.opentelemetry.io/contrib/propagators/b3 v1.35.0
	go.opentelemetry.io/otel v1.35.0
	go.opentelemetry.io/otel/bridge/opencensus v1.35.0
	go.opentelemetry.io/otel/exporters/prometheus v0.57.0
	go.opentelemetry.io/otel/metric v1.35.0
	go.opentelemetry.io/otel/sdk v1.35.0
	go.opentelemetry.io/otel/sdk/metric v1.35.0
	go.opentelemetry.io/otel/trace v1.35.0
	go.uber.org/zap v1.27.0
	goa.design/goa/v3 v3.21.0
	goa.design/plugins/v3 v3.21.0
	golang.org/x/exp v0.0.0-20250408133849-7e4ce0ab07d0
	golang.org/x/sync v0.13.0
	google.golang.org/grpc v1.72.0
	gopkg.in/DataDog/dd-trace-go.v1 v1.72.2
	gopkg.in/Knetic/govaluate.v3 v3.0.0
	v.io/x/lib v0.1.21
)

require (
	github.com/DATA-DOG/go-sqlmock v1.5.2 // indirect
	github.com/DataDog/datadog-agent/comp/core/tagger/origindetection v0.64.2 // indirect
	github.com/DataDog/datadog-agent/pkg/version v0.64.2 // indirect
	github.com/bitfield/gotestdox v0.2.2 // indirect
	github.com/go-test/deep v1.1.1 // indirect
	github.com/onsi/ginkgo/v2 v2.22.2 // indirect
	github.com/onsi/gomega v1.36.2 // indirect
	github.com/shirou/gopsutil/v4 v4.25.3 // indirect
	github.com/spiffe/go-spiffe/v2 v2.5.0 // indirect
	github.com/zeebo/errs v1.4.0 // indirect
	go.opentelemetry.io/collector/featuregate v1.29.0 // indirect
	go.opentelemetry.io/collector/internal/telemetry v0.123.0 // indirect
	go.opentelemetry.io/contrib/bridges/otelzap v0.10.0 // indirect
	go.opentelemetry.io/otel/log v0.11.0 // indirect
)

require (
	github.com/DataDog/datadog-agent/pkg/obfuscate v0.64.2 // indirect
	github.com/DataDog/datadog-go/v5 v5.6.0 // indirect
	github.com/DataDog/sketches-go v1.4.7 // indirect
	github.com/Microsoft/go-winio v0.6.2 // indirect
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/dimfeld/httppath v0.0.0-20170720192232-ee938bf73598 // indirect
	github.com/dimfeld/httptreemux/v5 v5.5.0
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/fsnotify/fsnotify v1.9.0
	github.com/google/uuid v1.6.0 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/iancoleman/strcase v0.3.0
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/kballard/go-shellquote v0.0.0-20180428030007-95032a82bc51 // indirect
	github.com/kr/pty v1.1.8 // indirect
	github.com/leodido/go-urn v1.4.0 // indirect
	github.com/manveru/faker v0.0.0-20171103152722-9fbc68a78c4d // indirect
	github.com/matoous/go-nanoid/v2 v2.1.0
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20
	github.com/mgutz/ansi v0.0.0-20200706080929-d51e80ef957d // indirect
	github.com/mitchellh/mapstructure v1.5.1-0.20231216201459-8508981c8b6c
	github.com/philhofer/fwd v1.1.3-0.20240916144458-20a13a1f6b7c // indirect
	github.com/sergi/go-diff v1.3.2-0.20230802210424-5b0b94c5c0d3 // indirect
	github.com/subosito/gotenv v1.6.0 // indirect
	github.com/tinylib/msgp v1.2.5 // indirect
	go.uber.org/atomic v1.11.0
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/crypto v0.37.0
	golang.org/x/mod v0.24.0 // indirect
	golang.org/x/net v0.39.0
	golang.org/x/sys v0.32.0
	golang.org/x/term v0.31.0
	golang.org/x/text v0.24.0
	golang.org/x/time v0.11.0 // indirect
	golang.org/x/tools v0.32.0
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da
	google.golang.org/protobuf v1.36.6
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/yaml.v2 v2.4.0
	gopkg.in/yaml.v3 v3.0.1
	sigs.k8s.io/yaml v1.4.0 // indirect
)

require (
	cel.dev/expr v0.23.1 // indirect
	cloud.google.com/go v0.120.0 // indirect
	cloud.google.com/go/auth v0.16.0 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.8 // indirect
	cloud.google.com/go/compute/metadata v0.6.0 // indirect
	cloud.google.com/go/iam v1.5.0 // indirect
	cloud.google.com/go/kms v1.21.2 // indirect
	cloud.google.com/go/longrunning v0.6.6 // indirect
	cloud.google.com/go/monitoring v1.24.1 // indirect
	cloud.google.com/go/storage v1.51.0 // indirect
	dario.cat/mergo v1.0.1 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.18.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.9.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.11.1 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azkeys v1.3.1 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/internal v1.1.1 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.6.0 // indirect
	github.com/Azure/go-autorest v14.2.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest/to v0.4.1 // indirect
	github.com/AzureAD/microsoft-authentication-library-for-go v1.4.2 // indirect
	github.com/DataDog/appsec-internal-go v1.11.2 // indirect
	github.com/DataDog/datadog-agent/pkg/proto v0.64.2 // indirect
	github.com/DataDog/datadog-agent/pkg/remoteconfig/state v0.64.2 // indirect
	github.com/DataDog/datadog-agent/pkg/trace v0.64.2 // indirect
	github.com/DataDog/datadog-agent/pkg/util/log v0.64.2 // indirect
	github.com/DataDog/datadog-agent/pkg/util/scrubber v0.64.2 // indirect
	github.com/DataDog/go-libddwaf/v3 v3.5.4 // indirect
	github.com/DataDog/go-runtime-metrics-internal v0.0.4-0.20241206090539-a14610dc22b6 // indirect
	github.com/DataDog/go-sqllexer v0.1.3 // indirect
	github.com/DataDog/go-tuf v1.1.0-0.5.2 // indirect
	github.com/DataDog/gostackparse v0.7.0 // indirect
	github.com/DataDog/opentelemetry-mapping-go/pkg/otlp/attributes v0.26.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp v1.27.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric v0.51.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/internal/resourcemapping v0.51.0 // indirect
	github.com/Masterminds/semver/v3 v3.3.1 // indirect
	github.com/MichaelMure/go-term-text v0.3.1 // indirect
	github.com/a8m/envsubst v1.4.3 // indirect
	github.com/aclements/go-moremath v0.0.0-20210112150236-f10218a38794 // indirect
	github.com/agext/levenshtein v1.2.3 // indirect
	github.com/air-verse/air v1.61.7 // indirect
	github.com/alecthomas/chroma v0.10.0 // indirect
	github.com/andybalholm/brotli v1.1.1 // indirect
	github.com/apex/log v1.9.0 // indirect
	github.com/apparentlymart/go-textseg/v15 v15.0.0 // indirect
	github.com/aws/aws-sdk-go v1.55.6 // indirect
	github.com/aws/aws-sdk-go-v2 v1.36.3 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.6.10 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.29.14 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.17.67 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.16.30 // indirect
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.17.72 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.34 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.34 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.3 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.3.34 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.12.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.7.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.12.15 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.18.15 // indirect
	github.com/aws/aws-sdk-go-v2/service/kms v1.38.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/s3 v1.79.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.25.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.30.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.33.19 // indirect
	github.com/aws/smithy-go v1.22.3 // indirect
	github.com/benbjohnson/clock v1.3.5 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bep/godartsass/v2 v2.5.0 // indirect
	github.com/bep/golibsass v1.2.0 // indirect
	// github.com/bitfield/gotestdox v0.2.2 // indirect
	github.com/bitly/go-simplejson v0.5.1 // indirect
	github.com/bmatcuk/doublestar v1.3.4 // indirect
	github.com/briandowns/spinner v1.16.0 // indirect
	github.com/bsm/redislock v0.9.4 // indirect
	github.com/cihub/seelog v0.0.0-20170130134532-f561c5e57575 // indirect
	github.com/cncf/xds/go v0.0.0-20250326154945-ae57f3c0d45f // indirect
	github.com/coreos/go-oidc/v3 v3.14.1 // indirect
	github.com/coreos/go-semver v0.3.1 // indirect
	github.com/coreos/go-systemd v0.0.0-20191104093116-d3cd4ed1dbcf // indirect
	github.com/coreos/go-systemd/v22 v22.5.0 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.6 // indirect
	github.com/creack/pty v1.1.24 // indirect
	github.com/daixiang0/gci v0.13.6 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/disintegration/imaging v1.6.2 // indirect
	github.com/dlclark/regexp2 v1.11.5 // indirect
	github.com/dnephin/pflag v1.0.7 // indirect
	github.com/eapache/queue/v2 v2.0.0-20230407133247-75960ed334e4 // indirect
	github.com/ebitengine/purego v0.8.2 // indirect
	github.com/eliukblau/pixterm/pkg/ansimage v0.0.0-20191210081756-9fb6cf8c2f75 // indirect
	github.com/envoyproxy/go-control-plane/envoy v1.32.4 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.2.1 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/gabriel-vasile/mimetype v1.4.8 // indirect
	github.com/ghodss/yaml v1.0.1-0.20220118164431-d8423dcdf344 // indirect
	github.com/go-chi/chi/v5 v5.2.1 // indirect
	github.com/go-jose/go-jose/v3 v3.0.4 // indirect
	github.com/go-jose/go-jose/v4 v4.0.5 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/go-openapi/inflect v0.21.2 // indirect
	github.com/go-ozzo/ozzo-routing v2.1.4+incompatible // indirect
	github.com/go-viper/mapstructure/v2 v2.2.1 // indirect
	github.com/gobwas/glob v0.2.3 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/gohugoio/hashstructure v0.5.0 // indirect
	github.com/gohugoio/hugo v0.146.7 // indirect
	github.com/golang-jwt/jwt/v4 v4.5.2 // indirect
	github.com/golang-jwt/jwt/v5 v5.2.2 // indirect
	github.com/golang/gddo v0.0.0-20210115222349-20d68f94ee1f // indirect
	github.com/golang/groupcache v0.0.0-20241129210726-2c02b8208cf8 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/gomarkdown/markdown v0.0.0-20250311123330-531bef5e742b // indirect
	github.com/google/btree v1.1.3 // indirect
	github.com/google/go-licenses/v2 v2.0.0-alpha.1 // indirect
	github.com/google/licenseclassifier/v2 v2.0.0 // indirect
	github.com/google/pprof v0.0.0-20250403155104-27863c87afa6 // indirect
	github.com/google/s2a-go v0.1.9 // indirect
	github.com/google/wire v0.6.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.6 // indirect
	github.com/googleapis/gax-go/v2 v2.14.1 // indirect
	github.com/gorilla/mux v1.8.1 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.4.0 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.16.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.26.3 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-secure-stdlib/parseutil v0.2.0 // indirect
	github.com/hashicorp/go-secure-stdlib/strutil v0.1.2 // indirect
	github.com/hashicorp/go-sockaddr v1.0.7 // indirect
	github.com/hashicorp/go-version v1.7.0 // indirect
	github.com/hashicorp/hcl/v2 v2.23.0 // indirect
	github.com/hexops/gotextdiff v1.0.3 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/justinas/alice v1.2.0 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/klauspost/reedsolomon v1.12.4 // indirect
	github.com/kyokomi/emoji/v2 v2.2.13 // indirect
	github.com/lucasb-eyer/go-colorful v1.2.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20250317134145-8bc96cf8fc35 // indirect
	github.com/mailru/easyjson v0.9.0 // indirect
	github.com/mattn/go-runewidth v0.0.16 // indirect
	github.com/mbland/hmacauth v0.0.0-20170912233209-44256dfd4bfa // indirect
	github.com/mightyguava/jl v0.1.0 // indirect
	github.com/minio/blake2b-simd v0.0.0-20160723061019-3f5f724cb5b1 // indirect
	github.com/mitchellh/colorstring v0.0.0-20190213212951-d06e56a500db // indirect
	github.com/mitchellh/go-wordwrap v1.0.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/oligot/go-mod-upgrade v0.11.0 // indirect
	github.com/otiai10/copy v1.14.0 // indirect
	github.com/outcaste-io/ristretto v0.2.3 // indirect
	github.com/pelletier/go-toml v1.9.5 // indirect
	github.com/pelletier/go-toml/v2 v2.2.4 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/pkg/browser v0.0.0-20240102092130-5ac0b6a4141c // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/power-devops/perfstat v0.0.0-20240221224432-82ca36839d55 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.63.0 // indirect
	github.com/prometheus/procfs v0.16.0 // indirect
	github.com/redis/go-redis/v9 v9.7.3 // indirect
	github.com/richardartoul/molecule v1.0.1-0.20240531184615-7ca0df43c0b3 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/rs/xid v1.6.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/ryanuber/go-glob v1.0.0 // indirect
	github.com/sagikazarmark/locafero v0.9.0 // indirect
	github.com/secure-systems-lab/go-securesystemslib v0.9.0 // indirect
	github.com/soheilhy/cmux v0.1.5 // indirect
	github.com/sourcegraph/conc v0.3.0 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/tdewolff/minify/v2 v2.21.3 // indirect
	github.com/tdewolff/parse/v2 v2.7.20 // indirect
	github.com/templexxx/cpu v0.1.1 // indirect
	github.com/templexxx/xorsimd v0.4.3 // indirect
	github.com/tjfoc/gmsm v1.4.1 // indirect
	github.com/tklauser/go-sysconf v0.3.15 // indirect
	github.com/tklauser/numcpus v0.10.0 // indirect
	github.com/tmc/grpc-websocket-proxy v0.0.0-20220101234140-673ab2c3ae75 // indirect
	github.com/urfave/cli/v2 v2.27.6 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/vmihailenco/msgpack/v5 v5.4.1 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	github.com/xiang90/probing v0.0.0-20221125231312-a49e3df8f510 // indirect
	github.com/xrash/smetrics v0.0.0-20240521201337-686a1a2994c1 // indirect
	github.com/yuin/goldmark v1.7.10 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	github.com/zclconf/go-cty v1.16.2 // indirect
	github.com/zclconf/go-cty-yaml v1.1.0 // indirect
	go.etcd.io/bbolt v1.4.0 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.21 // indirect
	go.etcd.io/etcd/client/v2 v2.305.21 // indirect
	go.etcd.io/etcd/pkg/v3 v3.5.21 // indirect
	go.etcd.io/etcd/raft/v3 v3.5.21 // indirect
	go.etcd.io/etcd/server/v3 v3.5.21 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/collector/component v1.29.0 // indirect
	go.opentelemetry.io/collector/pdata v1.29.0 // indirect
	go.opentelemetry.io/collector/pdata/pprofile v0.123.0 // indirect
	go.opentelemetry.io/collector/semconv v0.123.0 // indirect
	go.opentelemetry.io/contrib/detectors/gcp v1.35.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.35.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.35.0 // indirect
	go.opentelemetry.io/proto/otlp v1.5.0 // indirect
	gocloud.dev v0.41.0 // indirect
	golang.org/x/image v0.26.0 // indirect
	golang.org/x/oauth2 v0.29.0 // indirect
	golang.org/x/perf v0.0.0-20250414141303-3fc2b901edf3 // indirect
	google.golang.org/api v0.229.0 // indirect
	google.golang.org/genproto v0.0.0-20250409194420-de1ac958c67a // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20250414145226-207652e42e2e // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250428153025-10db94c68c34 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7 // indirect
	gotest.tools/gotestsum v1.12.1 // indirect
	k8s.io/apimachinery v0.32.3 // indirect
	k8s.io/klog/v2 v2.130.1 // indirect
	mvdan.cc/gofumpt v0.8.0 // indirect
)

tool (
	github.com/air-verse/air
	github.com/daixiang0/gci
	github.com/google/go-licenses/v2
	github.com/mightyguava/jl/cmd/jl
	github.com/oligot/go-mod-upgrade
	goa.design/goa/v3/cmd/goa
	golang.org/x/perf/cmd/benchstat
	golang.org/x/tools/cmd/godoc
	gotest.tools/gotestsum
	mvdan.cc/gofumpt
)
