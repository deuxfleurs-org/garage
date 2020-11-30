talk.pdf: talk.tex
	pdflatex talk.tex
	
fig_%.pdf: fig_%.svg
	inkscape -D -z --file=$^ --export-pdf=$@ --export-latex

