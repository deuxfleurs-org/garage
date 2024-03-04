ASSETS=../assets/logos/deuxfleurs.pdf

talk.pdf: talk.tex $(ASSETS)
	pdflatex talk.tex

%.pdf: %.svg
	inkscape -D -z --file=$^ --export-pdf=$@

%.pdf_tex: %.svg
	inkscape -D -z --file=$^ --export-pdf=$@ --export-latex
