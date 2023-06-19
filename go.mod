module github.com/keboola/keboola-as-code

go 1.19

replace github.com/google/go-jsonnet v0.19.1 => github.com/keboola/go-jsonnet v0.19.1

// https://github.com/etcd-io/etcd/issues/15877
replace google.golang.org/grpc v1.55.0 => google.golang.org/grpc v1.54.1

require (
	ariga.io/atlas v0.10.1
	entgo.io/ent v0.12.1
	github.com/ActiveState/vt10x v1.3.1
	github.com/AlecAivazis/survey/v2 v2.3.6
	github.com/Masterminds/semver v1.5.0
	github.com/MichaelMure/go-term-markdown v0.1.4
	github.com/Netflix/go-expect v0.0.0-20220104043353-73e0943537d2
	github.com/acarl005/stripansi v0.0.0-20180116102854-5a71ef0e047d
	github.com/armon/go-radix v1.0.0
	github.com/benbjohnson/clock v1.3.3
	github.com/c2h5oh/datasize v0.0.0-20220606134207-859f65c6625b
	github.com/cenkalti/backoff/v4 v4.2.1
	github.com/davecgh/go-spew v1.1.1
	github.com/fatih/color v1.15.0
	github.com/go-playground/locales v0.14.1
	github.com/go-playground/universal-translator v0.18.1
	github.com/go-playground/validator/v10 v10.14.0
	github.com/go-resty/resty/v2 v2.7.0
	github.com/google/go-cmp v0.5.9
	github.com/google/go-jsonnet v0.19.1
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510
	github.com/jarcoal/httpmock v1.3.0
	github.com/joho/godotenv v1.5.1
	github.com/jpillora/longestcommon v0.0.0-20161227235612-adb9d91ee629
	github.com/keboola/go-client v1.16.3
	github.com/keboola/go-utils v0.8.1
	github.com/klauspost/pgzip v1.2.6
	github.com/kylelemons/godebug v1.1.0
	github.com/lafikl/consistent v0.0.0-20220512074542-bdd3606bfc3e
	github.com/lestrrat-go/strftime v1.0.6
	github.com/mattn/go-sqlite3 v1.14.16
	github.com/prometheus/client_golang v1.15.1
	github.com/qmuntal/stateless v1.6.3
	github.com/santhosh-tekuri/jsonschema/v5 v5.3.0
	github.com/schollz/progressbar/v3 v3.13.1
	github.com/sirupsen/logrus v1.9.2
	github.com/spf13/afero v1.9.5
	github.com/spf13/cast v1.5.1
	github.com/spf13/cobra v1.7.0
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.15.0
	github.com/stretchr/testify v1.8.3
	github.com/umisama/go-regexpcache v0.0.0-20150417035358-2444a542492f
	github.com/urfave/negroni v1.0.0
	github.com/writeas/go-strip-markdown v2.0.1+incompatible
	go.etcd.io/etcd/api/v3 v3.5.9
	go.etcd.io/etcd/client/v3 v3.5.9
	go.etcd.io/etcd/tests/v3 v3.5.9
	go.nhat.io/aferocopy/v2 v2.0.1
	go.opencensus.io v0.24.0
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.42.0
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.42.0
	go.opentelemetry.io/contrib/propagators/b3 v1.17.0
	go.opentelemetry.io/otel v1.16.0
	go.opentelemetry.io/otel/bridge/opencensus v0.39.0
	go.opentelemetry.io/otel/exporters/prometheus v0.39.0
	go.opentelemetry.io/otel/metric v1.16.0
	go.opentelemetry.io/otel/sdk v1.16.0
	go.opentelemetry.io/otel/sdk/metric v0.39.0
	go.opentelemetry.io/otel/trace v1.16.0
	go.uber.org/zap v1.24.0
	goa.design/goa/v3 v3.11.1
	goa.design/plugins/v3 v3.11.1
	golang.org/x/sync v0.2.0
	google.golang.org/grpc v1.55.0
	gopkg.in/DataDog/dd-trace-go.v1 v1.50.1
	gopkg.in/Knetic/govaluate.v3 v3.0.0
	v.io/x/lib v0.1.14
)

require (
	github.com/DataDog/datadog-agent/pkg/obfuscate v0.43.1 // indirect
	github.com/DataDog/datadog-go/v5 v5.1.1 // indirect
	github.com/DataDog/sketches-go v1.4.1 // indirect
	github.com/Microsoft/go-winio v0.6.0 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dimfeld/httppath v0.0.0-20170720192232-ee938bf73598 // indirect
	github.com/dimfeld/httptreemux/v5 v5.5.0
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/fsnotify/fsnotify v1.6.0 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/gorilla/websocket v1.5.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/iancoleman/strcase v0.2.0
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/kballard/go-shellquote v0.0.0-20180428030007-95032a82bc51 // indirect
	github.com/kr/pty v1.1.8 // indirect
	github.com/leodido/go-urn v1.2.4 // indirect
	github.com/magiconair/properties v1.8.7 // indirect
	github.com/manveru/faker v0.0.0-20171103152722-9fbc68a78c4d // indirect
	github.com/matoous/go-nanoid/v2 v2.0.0
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.19
	github.com/mgutz/ansi v0.0.0-20200706080929-d51e80ef957d // indirect
	github.com/mitchellh/mapstructure v1.5.0
	github.com/philhofer/fwd v1.1.1 // indirect
	github.com/sergi/go-diff v1.3.1 // indirect
	github.com/spf13/jwalterweatherman v1.1.0 // indirect
	github.com/subosito/gotenv v1.4.2 // indirect
	github.com/tinylib/msgp v1.1.6 // indirect
	github.com/zach-klippenstein/goregen v0.0.0-20160303162051-795b5e3961ea // indirect
	go.uber.org/atomic v1.11.0
	go.uber.org/multierr v1.9.0 // indirect
	golang.org/x/crypto v0.7.0 // indirect
	golang.org/x/mod v0.10.0 // indirect
	golang.org/x/net v0.10.0 // indirect
	golang.org/x/sys v0.8.0 // indirect
	golang.org/x/term v0.8.0
	golang.org/x/text v0.9.0
	golang.org/x/time v0.3.0 // indirect
	golang.org/x/tools v0.9.1
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2
	google.golang.org/protobuf v1.30.0 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/yaml.v2 v2.4.0
	gopkg.in/yaml.v3 v3.0.1
	sigs.k8s.io/yaml v1.3.0 // indirect
)

require (
	cloud.google.com/go v0.110.0 // indirect
	cloud.google.com/go/compute v1.18.0 // indirect
	cloud.google.com/go/compute/metadata v0.2.3 // indirect
	cloud.google.com/go/iam v0.12.0 // indirect
	cloud.google.com/go/storage v1.30.1 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.6.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.2.1 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.3.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.0.0 // indirect
	github.com/Azure/go-autorest v14.2.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest/to v0.4.0 // indirect
	github.com/AzureAD/microsoft-authentication-library-for-go v0.8.1 // indirect
	github.com/DataDog/appsec-internal-go v1.0.0 // indirect
	github.com/DataDog/datadog-agent/pkg/remoteconfig/state v0.45.0-rc.1 // indirect
	github.com/DataDog/go-libddwaf v1.1.0 // indirect
	github.com/DataDog/go-tuf v0.3.0--fix-localmeta-fork // indirect
	github.com/MichaelMure/go-term-text v0.3.1 // indirect
	github.com/agext/levenshtein v1.2.1 // indirect
	github.com/alecthomas/chroma v0.10.0 // indirect
	github.com/andybalholm/brotli v1.0.5 // indirect
	github.com/apparentlymart/go-textseg/v13 v13.0.0 // indirect
	github.com/aws/aws-sdk-go v1.44.262 // indirect
	github.com/aws/aws-sdk-go-v2 v1.18.0 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.4.10 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.18.25 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.13.24 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.13.3 // indirect
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.11.51 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.1.33 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.4.27 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.3.34 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.0.25 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.9.11 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.1.28 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.9.27 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.14.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/s3 v1.33.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.12.10 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.14.10 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.19.0 // indirect
	github.com/aws/smithy-go v1.13.5 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd/v22 v22.5.0 // indirect
	github.com/creack/pty v1.1.18 // indirect
	github.com/disintegration/imaging v1.6.2 // indirect
	github.com/dlclark/regexp2 v1.7.0 // indirect
	github.com/eliukblau/pixterm/pkg/ansimage v0.0.0-20191210081756-9fb6cf8c2f75 // indirect
	github.com/felixge/httpsnoop v1.0.3 // indirect
	github.com/gabriel-vasile/mimetype v1.4.2 // indirect
	github.com/go-logr/logr v1.2.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-openapi/inflect v0.19.0 // indirect
	github.com/gofrs/flock v0.8.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-jwt/jwt/v4 v4.4.3 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/gomarkdown/markdown v0.0.0-20221013030248-663e2500819c // indirect
	github.com/google/btree v1.0.1 // indirect
	github.com/google/wire v0.5.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.2.3 // indirect
	github.com/googleapis/gax-go/v2 v2.8.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.16.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.11.3 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/hcl/v2 v2.13.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/jonboulle/clockwork v0.2.2 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.15.1 // indirect
	github.com/kyokomi/emoji/v2 v2.2.10 // indirect
	github.com/lucasb-eyer/go-colorful v1.2.0 // indirect
	github.com/mattn/go-runewidth v0.0.14 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/minio/blake2b-simd v0.0.0-20160723061019-3f5f724cb5b1 // indirect
	github.com/mitchellh/colorstring v0.0.0-20190213212951-d06e56a500db // indirect
	github.com/mitchellh/go-wordwrap v1.0.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/outcaste-io/ristretto v0.2.1 // indirect
	github.com/pelletier/go-toml/v2 v2.0.6 // indirect
	github.com/pkg/browser v0.0.0-20210911075715-681adbf594b8 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.4.0 // indirect
	github.com/prometheus/common v0.42.0 // indirect
	github.com/prometheus/procfs v0.9.0 // indirect
	github.com/relvacode/iso8601 v1.3.0 // indirect
	github.com/rivo/uniseg v0.4.3 // indirect
	github.com/secure-systems-lab/go-securesystemslib v0.5.0 // indirect
	github.com/soheilhy/cmux v0.1.5 // indirect
	github.com/tmc/grpc-websocket-proxy v0.0.0-20201229170055-e5319fda7802 // indirect
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	github.com/zclconf/go-cty v1.8.0 // indirect
	go.etcd.io/bbolt v1.3.7 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.9 // indirect
	go.etcd.io/etcd/client/v2 v2.305.9 // indirect
	go.etcd.io/etcd/pkg/v3 v3.5.9 // indirect
	go.etcd.io/etcd/raft/v3 v3.5.9 // indirect
	go.etcd.io/etcd/server/v3 v3.5.9 // indirect
	go.opentelemetry.io/otel/exporters/otlp/internal/retry v1.15.1 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.15.1 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.15.1 // indirect
	go.opentelemetry.io/proto/otlp v0.19.0 // indirect
	go4.org/intern v0.0.0-20220617035311-6925f38cc365 // indirect
	go4.org/unsafe/assume-no-moving-gc v0.0.0-20220617031537-928513b29760 // indirect
	gocloud.dev v0.29.0 // indirect
	golang.org/x/image v0.5.0 // indirect
	golang.org/x/oauth2 v0.8.0 // indirect
	google.golang.org/api v0.114.0 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20230320184635-7606e756e683 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0 // indirect
	inet.af/netaddr v0.0.0-20220811202034-502d2d690317 // indirect
)
