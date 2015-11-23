from snakemake.utils import min_version
from snakemake.remote.HTTP import RemoteProvider as HTTPRemoteProvider

min_version ('3.5')

HTTP = HTTPRemoteProvider()

rule all:
    input: 'output/shaded.relief/2015/L323/L3231D2.tif'

rule hillshade:
    input: 'output/dem/{year}/{region}/{sheet}.tif'
    output: 'output/shaded.relief/{year}/{region}/{sheet}.tif'

rule dem:
    input: 'output/xyz/{year}/{region}/{sheet}.xyz'
    output: rules.hillshade.input

rule xyz:
    input: 'las/{year}/{region}/{sheet}.las'
    output: rules.dem.input

rule las:
    input: 'laz/{year}/{region}/{sheet}.laz'
    output: rules.xyz.input
    shell: "las2las --input {input} --output {output} --keep-classes 2 9"

rule laz:
    input: HTTP.remote ('kartat.kapsi.fi/files/laser/etrs-tm35fin-n2000/mara_2m/{year}/{region}/1/{sheet}.laz', insecure = True)
    output: rules.las.input
    shell: 'mv "{input}" "{output}"'
