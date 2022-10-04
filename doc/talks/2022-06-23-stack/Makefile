talk.pdf: talk.tex assets/consistent_hashing_1.pdf assets/consistent_hashing_2.pdf assets/consistent_hashing_3.pdf assets/consistent_hashing_4.pdf assets/garage_tables.pdf assets/deuxfleurs.pdf
	pdflatex talk.tex

assets/%.pdf: assets/%.svg
	inkscape -D -z --file=$^ --export-pdf=$@
