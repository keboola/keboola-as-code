import os
paths="""${{ inputs.paths }}"""
splited=paths.splitlines()
paths=[]
for p in splited:
  if os.path.exists(p):
    paths.append(p)

env_file = os.getenv('GITHUB_ENV')
with open(env_file, "w") as myfile:
    myfile.write("SED_PATHS="+" ".join(paths))

