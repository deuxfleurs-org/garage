ASSETS=../assets/deuxfleurs.pdf

talk.pdf: talk.tex $(ASSETS)
	pdflatex talk.tex

assets/%.pdf: assets/%.svg
	inkscape -D -z --file=$^ --export-pdf=$@

assets/%.pdf_tex: assets/%.svg
	inkscape -D -z --file=$^ --export-pdf=$@ --export-latex
