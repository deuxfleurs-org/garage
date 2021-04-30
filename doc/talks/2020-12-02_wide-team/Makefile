talk.pdf: talk.tex img/garage_distributed.pdf img/consistent_hashing_1.pdf img/consistent_hashing_2.pdf img/consistent_hashing_3.pdf img/consistent_hashing_4.pdf img/garage_tables.pdf
	pdflatex talk.tex
	
img/%.pdf: img/%.svg
	inkscape -D -z --file=$^ --export-pdf=$@

