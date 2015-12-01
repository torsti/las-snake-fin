from snakemake.utils import min_version, format
from snakemake.remote.HTTP import RemoteProvider as HTTPRemoteProvider

from subprocess import check_output
from re import search

min_version ('3.5')

HTTP = HTTPRemoteProvider()

SHEET_DICT = [{'name': 'L3231D2', 'year': '2015'}]
SHEETS = [s['year'] + '/' + s['name'][:4] + '/1/' + s['name'] for s in SHEET_DICT]
CLIPS = [s['name'][:2] + '/' + s['name'][:3] + '/' + s['name'][:5] + 'L' if s['name'][-2:-1] in ['A', 'B', 'C', 'D'] else 'R' + '_mtk' for s in SHEET_DICT]

def clipregion (wildcards):
    return ['mtk/' + c + '.shp' for c in CLIPS if wildcards.sheet[-7:-2] in c]

rule all:
    input: expand ('output/merged.relief/{sheet}.tif', sheet = SHEETS)

rule merged_relief:
    input:
        color_relief = 'output/color.relief/{sheet}.tif',
        hillshade = 'output/shaded.relief/{sheet}.tif',
        slope_relief = 'output/slope_relief/{sheet}.tif'
    output: 'output/merged.relief/{sheet}.tif'
    message: "Merge color and slope reliefs with hillshade."
    shell: '''
        listgeo {input.color_relief} > .meta.txt &&
        convert -gamma .5 {input.hillshade} .tmp.tif &&
        convert {input.color_relief} {input.slope_relief} .tmp.tif -compose Overlay -composite .tmp.tif &&
        geotifcp -g .meta.txt .tmp.tif {output} &&
        rm .meta.txt .tmp.tif'''

rule slope_relief:
    input: 'output/dem/{sheet}.tif'
    output: rules.merged_relief.input.slope_relief
    message: "Generate slope turn into slope color relief."
    shell: '''
        gdaldem slope {input} {output} &&
        gdaldem color-relief {output} color_slope.txt {output}
    '''

rule color_relief:
    input: 'output/dem/{sheet}.tif'
    output: rules.merged_relief.input.color_relief
    message: "Generate color-relief from DEM."
    shell: 'gdaldem color-relief "{input}" color_relief.txt "{output}"'

rule hillshade:
    input: 'output/dem/{sheet}.tif'
    output: rules.merged_relief.input.hillshade
    message: "Generate hillshade from DEM."
    shell: 'gdaldem hillshade "{input}" "{output}"'

rule dem:
    input:
        shp = 'output/shp/{sheet}.shp',
        las = 'output/las/{sheet}.las'
        #clip_region = clipregion
    output: rules.hillshade.input
    params:
        output_type = "Float32",
        algorithm = "nearest:radius1=10:radius2=10",
        resolution = "1"
    message: "Interpolate DEM from Shapefile points."
    run:
        lasinfo = check_output(['lasinfo', format ('{input.las}')])
        e = [str (round (float (c))) for c in search ('(\d+[.]\d+), (\d+[.]\d+), (\d+[.]\d+), (\d+[.]\d+)', [l.decode () for l in lasinfo.splitlines() if 'Bounding Box:' in l.decode ()][0]).groups ()]
        ye = e[1] + ' ' + e[3]
        xe = e[0] + ' ' + e[2]
        size = str ((int (e[3]) - int (e[1])) / float (params.resolution)) + ' ' + str ((int (e[2]) - int (e[0])) / float (params.resolution))
        epsg = [l.decode () for l in lasinfo.splitlines() if 'AUTHORITY' in l.decode ()][-1]
        epsg = epsg.replace('AUTHORITY', '').replace ('["', '').replace ('"]]', '').replace (",", ':')
        shell ('''gdal_grid \
        -a_srs {epsg} \
        -ot {params.output_type} \
        -of "GTiff" -a {params.algorithm} \
        -tye {ye} -txe {xe} \
        -outsize {size} \
        -l "$(basename -s .shp {input.shp})" \
        "{input.shp}" "{output}" \
        --config GDAL_NUM_THREADS {threads}
        ''')

rule shp:
    input: 'output/las/{sheet}.las'
    output: rules.dem.input.shp
    message: "Convert .las to Shapefile."
    shell: 'las2ogr -f "ESRI Shapefile" -i "{input}" -o "{output}"'

rule las:
    input: 'output/laz/{sheet}.laz'
    output: rules.dem.input.las
    message: "Decompress and extract ground classified points from .laz file."
    params:
        classes = "2"
    shell: 'las2las --input "{input}" --output "{output}" --keep-classes {params.classes}'

rule clip:
    input: HTTP.remote (expand ('kartat.kapsi.fi/files/maastotietokanta/kaikki/etrs89/gml/{clipregion}_mtk.zip', clipregion = CLIPS), insecure = True)
    output: expand ('mtk/{clipregion}.shp', clipregion = CLIPS)
    message: "Download and extract sea-areas from the topographic database."
    shell: 'ogr2ogr "{output}" "/vsizip/$(readlink -f {input})" Meri'

rule laz:
    input: HTTP.remote ('kartat.kapsi.fi/files/laser/etrs-tm35fin-n2000/mara_2m/{sheet}.laz', insecure = True)
    output: rules.las.input
    message: "Download laz sheet."
    shell: 'mv "{input}" "{output}"'

rule clean:
    message: "Remove generated files."
    shell: 'rm -rf output/ las/'

rule clean_remote:
    message: "Remove downloaded files."
    shell: 'rm -rf laz/ mtk/'
