ASSETS=assets/consistent_hashing_1.pdf \
	   assets/consistent_hashing_2.pdf \
	   assets/consistent_hashing_3.pdf \
	   assets/consistent_hashing_4.pdf \
	   assets/garage_tables.pdf \
	   assets/consensus.pdf_tex \
	   assets/lattice1.pdf_tex \
	   assets/lattice2.pdf_tex \
	   assets/lattice3.pdf_tex \
	   assets/lattice4.pdf_tex \
	   assets/lattice5.pdf_tex \
	   assets/lattice6.pdf_tex \
	   assets/lattice7.pdf_tex \
	   assets/lattice8.pdf_tex \
	   assets/latticeB_1.pdf_tex \
	   assets/latticeB_2.pdf_tex \
	   assets/latticeB_3.pdf_tex \
	   assets/latticeB_4.pdf_tex \
	   assets/latticeB_5.pdf_tex \
	   assets/latticeB_6.pdf_tex \
	   assets/latticeB_7.pdf_tex \
	   assets/latticeB_8.pdf_tex \
	   assets/latticeB_9.pdf_tex \
	   assets/latticeB_10.pdf_tex \
	   assets/deuxfleurs.pdf

talk.pdf: talk.tex $(ASSETS)
	pdflatex talk.tex

assets/%.pdf: assets/%.svg
	inkscape -D -z --file=$^ --export-pdf=$@

assets/%.pdf_tex: assets/%.svg
	inkscape -D -z --file=$^ --export-pdf=$@ --export-latex
