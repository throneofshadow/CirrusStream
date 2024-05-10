import sys, os

sys.path.append(os.path.abspath('sphinxext'))

extensions = ['extname']

source_suffix = {
    '.rst': 'restructuredtext',
    '.txt': 'restructuredtext',
    '.md': 'markdown',
}
source_parsers = {'.md': 'recommonmark.parser.CommonMarkParser'}
