.PHONY: all viewpdf pdf clean

TARGET       = main
SOURCE_FILES = $(shell find . -type f -name "*.tex" -print)
CLASS_FILES  = $(shell find . -type f -name "*.cls" -print)
BIB_FILES    = $(shell find . -type f -name "*.bib" -print)
FIGURES      = $(shell find . -path "./figures/*" -type f -print)
BUILD_PATH   = build
BUILD_FILES  = $(shell find $(BUILD_PATH) -type f -print)

BIB_PROCESSOR := biber

.PHONY: all pdf clean figures

all: pdf

pdf: $(TARGET).pdf

clean:
	@rm $(TARGET).pdf $(BUILD_FILES) > /dev/null 2>&1 || exit 0

figures: figures/crdt.pdf

$(TARGET).pdf: figures $(FIGURES) $(SOURCE_FILES) $(BIB_FILES) $(CLASS_FILES)
	@mkdir -p $(BUILD_PATH) > /dev/null 2>&1 || exit 0
	@pdflatex -interaction=nonstopmode -jobname=$(TARGET) -output-directory $(BUILD_PATH) $(TARGET).tex
	@$(BIB_PROCESSOR) --output-directory $(BUILD_PATH) $(TARGET)
	@pdflatex -interaction=nonstopmode -jobname=$(TARGET) -output-directory $(BUILD_PATH) $(TARGET).tex # For biber
	@pdflatex -interaction=nonstopmode -jobname=$(TARGET) -output-directory $(BUILD_PATH) $(TARGET).tex # For biber
	@ln -fs $(BUILD_PATH)/$(TARGET).pdf $(TARGET).pdf

figures/crdt.pdf: figures/svg/crdt.svg
	@inkscape -C --file=$< --export-pdf=$@