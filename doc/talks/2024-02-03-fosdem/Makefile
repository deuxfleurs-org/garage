ASSETS=../assets/lattice/lattice1.pdf_tex \
	   ../assets/lattice/lattice2.pdf_tex \
	   ../assets/lattice/lattice3.pdf_tex \
	   ../assets/lattice/lattice4.pdf_tex \
	   ../assets/lattice/lattice5.pdf_tex \
	   ../assets/lattice/lattice6.pdf_tex \
	   ../assets/lattice/lattice7.pdf_tex \
	   ../assets/lattice/lattice8.pdf_tex \
	   ../assets/logos/deuxfleurs.pdf \
	   ../assets/timeline-22-24.pdf

talk.pdf: talk.tex $(ASSETS)
	pdflatex talk.tex

%.pdf: %.svg
	inkscape -D -z --file=$^ --export-pdf=$@

%.pdf_tex: %.svg
	inkscape -D -z --file=$^ --export-pdf=$@ --export-latex
