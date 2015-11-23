from snakemake.utils import min_version, format
from snakemake.remote.HTTP import RemoteProvider as HTTPRemoteProvider

from subprocess import check_output
from re import search

min_version ('3.5')

HTTP = HTTPRemoteProvider()

rule all:
    input: 'output/merged.relief/2015/L323/L3231D2.tif'

rule merged_relief:
    input:
        color_relief = 'output/color.relief/{year}/{region}/{sheet}.tif',
        hillshade = 'output/shaded.relief/{year}/{region}/{sheet}.tif',
        slope_relief = 'output/slope_relief/{year}/{region}/{sheet}.tif'
    output: 'output/merged.relief/{year}/{region}/{sheet}.tif'
    shell: '''
        listgeo {input.color_relief} > .meta.txt &&
        convert -gamma .5 {input.hillshade} .tmp.tif &&
        convert {input.color_relief} {input.slope_relief} .tmp.tif -compose Overlay -composite .tmp.tif &&
        geotifcp -g .meta.txt .tmp.tif {output} &&
        rm .meta.txt .tmp.tif'''

rule slope_relief:
    input: 'output/dem/{year}/{region}/{sheet}.tif'
    output: rules.merged_relief.input.slope_relief
    shell: '''
        gdaldem slope {input} {output} &&
        gdaldem color-relief {output} color_slope.txt {output}
    '''



rule color_relief:
    input: 'output/dem/{year}/{region}/{sheet}.tif'
    output: rules.merged_relief.input.color_relief
    shell: 'gdaldem color-relief "{input}" color_relief.txt "{output}"'

rule hillshade:
    input: 'output/dem/{year}/{region}/{sheet}.tif'
    output: rules.merged_relief.input.hillshade
    shell: 'gdaldem hillshade "{input}" "{output}"'

rule dem:
    input:
        shp = 'output/shp/{year}/{region}/{sheet}.shp',
        las = 'las/{year}/{region}/{sheet}.las'
    output: rules.hillshade.input
    params:
        output_type = "Float32",
        algorithm = "nearest:radius1=10:radius2=10",
        resolution = "1"
    run:
        e = [str (round (float (c))) for c in search ('(\d+[.]\d+), (\d+[.]\d+), (\d+[.]\d+), (\d+[.]\d+)', [l.decode () for l in check_output(['lasinfo', format ('{input.las}')]).splitlines() if 'Bounding Box:' in l.decode ()][0]).groups ()]
        ye = e[1] + ' ' + e[3]
        xe = e[0] + ' ' + e[2]
        size = str ((int (e[3]) - int (e[1])) / float (params.resolution)) + ' ' + str ((int (e[2]) - int (e[0])) / float (params.resolution))
        shell ('''gdal_grid \
        -ot {params.output_type} \
        -of "GTiff" -a {params.algorithm} \
        -tye {ye} -txe {xe} \
        -outsize {size} \
        -l "$(basename -s .shp {input.shp})" \
        "{input.shp}" "{output}" \
        --config GDAL_NUM_THREADS {threads}
        ''')

rule shp:
    input: 'las/{year}/{region}/{sheet}.las'
    output: rules.dem.input.shp
    shell: 'las2ogr -f "ESRI Shapefile" -i "{input}" -o "{output}"'

rule las:
    input: 'laz/{year}/{region}/{sheet}.laz'
    output: rules.dem.input.las
    params:
        classes = "2"
    shell: 'las2las --input "{input}" --output "{output}" --keep-classes {params.classes}'

rule laz:
    input: HTTP.remote ('kartat.kapsi.fi/files/laser/etrs-tm35fin-n2000/mara_2m/{year}/{region}/1/{sheet}.laz', insecure = True)
    output: rules.las.input
    message: "Downloading"
    shell: 'mv "{input}" "{output}"'
