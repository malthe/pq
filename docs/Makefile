# Makefile for Sphinx documentation
#

# You can set these variables from the command line.
SPHINXOPTS      =
SPHINXBUILD     = sphinx-build
BUILDDIR        = _build
ALLSPHINXOPTS   = -d $(BUILDDIR)/doctrees -c . ../
SCSSBIN         = python -m scss
SCSSINPATH      = _themes/pq/scss/
SCSSOUTFILE     = _themes/pq/css/pq.css

.PHONY: clean html coverage

clean:
	-rm -rf $(BUILDDIR)/*

html:
	$(SPHINXBUILD) -b html $(ALLSPHINXOPTS) $(BUILDDIR)/html
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html."

all: html
