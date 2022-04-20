local part1 = import "lib/part1.jsonnet";
local part2 = import "/<common>/foo/bar/extractor/ex-generic-v2/myconfig/lib/part2.jsonnet";
std.mergePatch(part1, part2)
