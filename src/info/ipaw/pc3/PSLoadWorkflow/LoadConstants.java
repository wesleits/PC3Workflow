package info.ipaw.pc3.PSLoadWorkflow;

import java.util.*;

public class LoadConstants {
	/***
	 * List of tables expected to be present in a CSV Batch
	 */
	public static final List<String> EXPECTED_TABLES = 
		Arrays.asList(new String[] { "P2Detection", "P2FrameMeta", "P2ImageMeta" });

	/***
	 * List of CSV file/table columns for the Detections file/table
	 */
	public static final List<String> EXPECTED_DETECTION_COLS = 
		Arrays.asList(new String[] { 
					"objID", "detectID", "ippObjID",
					"ippDetectID", "filterID", "imageID", "obsTime", "xPos",
					"yPos", "xPosErr", "yPosErr", "instFlux", "instFluxErr",
					"psfWidMajor", "psfWidMinor", "psfTheta", "psfLikelihood",
					"psfCf", "infoFlag", "htmID", "zoneID", "assocDate",
					"modNum", "ra", "dec", "raErr", "decErr", "cx", "cy", "cz",
					"peakFlux", "calMag", "calMagErr", "calFlux", "calFluxErr",
					"calColor", "calColorErr", "sky", "skyErr", "sgSep",
					"dataRelease" 
					});

	/***
	 * List of CSV file/table columns for the FrameMeta file/table
	 */
	public static final List<String> EXPECTED_FRAME_META_COLS = Arrays
			.asList(new String[] { "frameID", "surveyID", "filterID",
					"cameraID", "telescopeID", "analysisVer", "p1Recip",
					"p2Recip", "p3Recip", "nP2Images", "astroScat",
					"photoScat", "nAstRef", "nPhoRef", "expStart", "expTime",
					"airmass", "raBore", "decBore" });

	/***
	 * List of CSV file/table columns for the ImageMeta file/table
	 */
	public static final List<String> EXPECTED_IMAGE_META_COLS = 
		Arrays.asList(new String[] { 
					"imageID", "frameID", "ccdID", "photoCalID",
					"filterID", "bias", "biasScat", "sky", "skyScat",
					"nDetect", "magSat", "completMag", "astroScat",
					"photoScat", "nAstRef", "nPhoRef", "nx", "ny", "psfFwhm",
					"psfModelID", "psfSigMajor", "psfSigMinor", "psfTheta",
					"psfExtra1", "psfExtra2", "apResid", "dapResid",
					"detectorID", "qaFlags", "detrend1", "detrend2",
					"detrend3", "detrend4", "detrend5", "detrend6", "detrend7",
					"detrend8", "photoZero", "photoColor", "projection1",
					"projection2", "crval1", "crval2", "crpix1", "crpix2",
					"pc001001", "pc001002", "pc002001", "pc002002",
					"polyOrder", "pca1x3y0", "pca1x2y1", "pca1x1y2",
					"pca1x0y3", "pca1x2y0", "pca1x1y1", "pca1x0y2", "pca2x3y0",
					"pca2x2y1", "pca2x1y2", "pca2x0y3", "pca2x2y0", "pca2x1y1",
					"pca2x0y2" 
					});

	/***
	 * 
	 *
	 */
	public static class ColumnRange {
		public ColumnRange(String ColumnName_, String MinValue_, String MaxValue_) {
			ColumnName = ColumnName_;
			MinValue = MinValue_;
			MaxValue = MaxValue_;
		}

		public String ColumnName;
		public String MinValue;
		public String MaxValue;
	}

	/***
	 * 
	 */
	public static final List<ColumnRange> EXPECTED_DETECTION_COL_RANGES = 
		Arrays.asList(new ColumnRange[] { 
					new ColumnRange("ra", "0", "360"),
					new ColumnRange("\"dec\"", "-90", "90"),
					new ColumnRange("raErr", "-2000", "9"),
					new ColumnRange("decErr", "0", "9"), 
					});
}
