package org.broadinstitute.hellbender.tools.funcotator;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.BetaFeature;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.barclay.help.DocumentedFeature;
import org.broadinstitute.hellbender.engine.GATKTool;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.io.IOUtils;
import picard.cmdline.programgroups.VariantEvaluationProgramGroup;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Tool to download the latest data sources for {@link Funcotator}.
 * Created by jonn on 8/23/18.
 */
@CommandLineProgramProperties(
        summary = "Download the Broad Institute pre-packaged data sources for the somatic or germline use case for Funcotator.",
        oneLineSummary = "Data source downloader for Funcotator.",
        programGroup = VariantEvaluationProgramGroup.class
)
@DocumentedFeature
@BetaFeature
public class FuncotatorDataSourceRetriever extends GATKTool {

    private static final Logger logger = LogManager.getLogger(Funcotator.class);

    //==================================================================================================================
    // Public Static Members:

    public static final String VALIDATE_INTEGRITY_ARG_LONG_NAME = "validate-integrity";
    public static final String SOMATIC_ARG_LONG_NAME = "somatic";
    public static final String GERMLINE_ARG_LONG_NAME = "germline";

    //==================================================================================================================
    // Private Static Members:

    private static String GERMLINE_GCLOUD_DATASOURCES_BASEURL     = "gs://broad-public-datasets/funcotator/funcotator_dataSources.v1.4.20180615";
    private static Path   GERMLINE_GCLOUD_DATASOURCES_PATH        = IOUtils.getPath(GERMLINE_GCLOUD_DATASOURCES_BASEURL + ".tar.gz");
    private static Path   GERMLINE_GCLOUD_DATASOURCES_SHA256_PATH = IOUtils.getPath(GERMLINE_GCLOUD_DATASOURCES_BASEURL + ".sha256");

    //==================================================================================================================
    // Private Members:

    @Argument(fullName = VALIDATE_INTEGRITY_ARG_LONG_NAME,
            shortName = VALIDATE_INTEGRITY_ARG_LONG_NAME,
            doc = "Validate the integrity of the data sources after downloading them using sha256.", optional = true)
    private boolean validateIntegrity = false;

    @Argument(fullName = SOMATIC_ARG_LONG_NAME,
            shortName = SOMATIC_ARG_LONG_NAME,
            mutex = {GERMLINE_ARG_LONG_NAME},
            doc = "Download the latest pre-packaged datasources for somatic functional annotation.", optional = true)
    private boolean getSomaticDataSources = false;

    @Argument(fullName = GERMLINE_ARG_LONG_NAME,
            shortName = GERMLINE_ARG_LONG_NAME,
            mutex = {SOMATIC_ARG_LONG_NAME},
            doc = "Download the latest pre-packaged datasources for germline functional annotation.", optional = true)
    private boolean getGermlineDataSources = false;

    //==================================================================================================================
    // Constructors:

    //==================================================================================================================
    // Override Methods:


    @Override
    protected void onStartup() {
        super.onStartup();

        // Make sure the user specified at least one data source to download:
        if ((!getSomaticDataSources) && (!getGermlineDataSources)) {
            throw new UserException("Must select either somatic or germline datasources.");
        }
    }

    @Override
    public void traverse() {

        // Get the correct data source:
        if ( getSomaticDataSources ) {
            logger.info("Somatic data sources selected.");

        }

        if ( getGermlineDataSources ) {

            logger.info("Germline data sources selected.");

            // Get the germline datasources file:
            final Path germlineDataSources = downloadDatasources(GERMLINE_GCLOUD_DATASOURCES_PATH);

            // Validate the data sources if requested:
            if (validateIntegrity) {
                logger.info("Integrity validation selected.");
                validateIntegrity(germlineDataSources, GERMLINE_GCLOUD_DATASOURCES_SHA256_PATH);
            }
        }
    }

    //==================================================================================================================
    // Static Methods:

    //==================================================================================================================
    // Instance Methods:

    private Path downloadDatasources(final Path datasourcesPath) {

        // Get the data sources file:
        final Path outputDestination = getDataSourceLocalPath(datasourcesPath);
        try {
            logger.info("Initiating download of datasources from " + datasourcesPath.toUri().toString() + " to " + outputDestination.toUri().toString());
            logger.info("Please wait.  This will take a while...");
            java.nio.file.Files.copy(datasourcesPath, outputDestination);
            logger.info("Download Complete!");
        }
        catch (final IOException ex) {
            throw new GATKException("Could not copy data sources file: " + datasourcesPath.toUri().toString() + " -> " + outputDestination.toUri().toString(), ex);
        }

        return outputDestination;
    }

    private void validateIntegrity(final Path localDataSourcesPath, final Path remoteSha256Path) {

        // Get the SHA 256 file:
        final Path localSha256Path = getDataSourceLocalPath(remoteSha256Path);
        try {
            logger.info("Retrieving expected checksum file...");
            java.nio.file.Files.copy(remoteSha256Path, localSha256Path);
            logger.info("File transfer complete!");
        }
        catch (final IOException ex) {
            throw new GATKException("Could not copy sha256 sum from server: " + remoteSha256Path.toUri().toString() + " -> " + localSha256Path.toUri().toString(), ex);
        }

        // Read the sha256sum into memory:
        final String expectedSha256Sum;
        try {
            logger.info("Collecting expected checksum...");
            expectedSha256Sum = FileUtils.readFileToString(localSha256Path.toFile(), Charset.defaultCharset());
            logger.info("Collection complete!");
        }
        catch ( final IOException ex ) {
            throw new GATKException("Could not read in sha256sum from file: " + localSha256Path.toUri().toString(), ex);
        }

        // Calculate the sha256sum of the data sources file:
        final String actualSha256Sum;
        try (final InputStream dataStream = Files.newInputStream(localDataSourcesPath) ) {
            logger.info("Calculating sha256sum...");
            actualSha256Sum = DigestUtils.sha256Hex(dataStream);
            logger.info("Calculation complete!");
        }
        catch ( final IOException ex ) {
            throw new GATKException("Could not read downloaded data sources file to calculate hash: " + localDataSourcesPath.toUri().toString(), ex);
        }

        // verify the hashes are the same:
        if ( !expectedSha256Sum.equals(actualSha256Sum) ) {
            throw new GATKException("ERROR: downloaded data sources are corrupt!  Unexpected checksum: " + actualSha256Sum + " != " + expectedSha256Sum);
        }
        else {
            logger.info("Data sources are valid.");
        }
    }

    private Path getDataSourceLocalPath(final Path dataSourcesPath) {
        return IOUtils.getPath(dataSourcesPath.getFileName().toString());
    }

    //==================================================================================================================
    // Helper Data Types:

}
