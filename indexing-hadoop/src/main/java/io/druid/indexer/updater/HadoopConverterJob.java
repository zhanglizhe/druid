/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexer.updater;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.annotations.Self;
import io.druid.indexer.MetadataStorageUpdaterJobHandler;
import io.druid.initialization.Initialization;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMaker;
import io.druid.segment.ProgressIndicator;
import io.druid.segment.SegmentUtils;
import io.druid.server.DruidNode;
import io.druid.timeline.DataSegment;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.util.Progressable;
import org.joda.time.format.ISODateTimeFormat;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class HadoopConverterJob
{
  private static final Logger log = new Logger(HadoopConverterJob.class);
  private static final String COUNTER_GROUP = "Hadoop Druid Converter";
  private static final String COUNTER_LOADED = "Loaded Bytes";
  private static final String COUNTER_WRITTEN = "Written Bytes";
  private static Injector injector = null;
  private final HadoopDruidConverterConfig converterConfig;
  private long loadedBytes = 0L;
  private long writtenBytes = 0L;

  public HadoopConverterJob(
      HadoopDruidConverterConfig converterConfig
  )
  {
    this.converterConfig = converterConfig;
  }

  private static boolean interestingFile(final File file)
  {
    return file != null && (!file.isDirectory()) &&
           (file.getName().endsWith(".jar"));
  }

  private static Iterable<File> findClassFiles(final File dir) throws IOException
  {
    if (dir == null) {
      return ImmutableList.of();
    }
    if (!dir.isDirectory()) {
      if (interestingFile(dir)) {
        return ImmutableList.of(dir);
      } else {
        return ImmutableList.of();
      }
    }
    final File[] files = dir.listFiles();
    if (files == null) {
      return ImmutableList.of();
    }
    return Iterables.concat(
        Iterables.transform(
            Arrays.asList(files),
            new Function<File, Iterable<File>>()
            {
              @Nullable
              @Override
              public Iterable<File> apply(File input)
              {
                if (input.isDirectory()) {
                  try {
                    return findClassFiles(input);
                  }
                  catch (IOException e) {
                    throw Throwables.propagate(e);
                  }
                }
                if (interestingFile(input)) {
                  return ImmutableList.of(input);
                } else {
                  return ImmutableList.of();
                }
              }
            }
        )
    );
  }

  public List<DataSegment> run() throws IOException
  {
    final JobConf jobConf = new JobConf();
    jobConf.setKeepFailedTaskFiles(false);
    for (Map.Entry<String, String> entry : converterConfig.getHadoopProperties().entrySet()) {
      jobConf.set(entry.getKey(), entry.getValue(), "converterConfig.getHadoopProperties()");
    }
    final List<DataSegment> segments = converterConfig.getSegments();
    if (segments.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "No segments found for datasource [%s]",
              converterConfig.getDataSource()
          )
      );
    }
    converterConfigIntoConfiguration(converterConfig, segments, jobConf);
    jobConf.setNumMapTasks(segments.size());
    jobConf.setNumReduceTasks(0);
    jobConf.setWorkingDirectory(new Path(converterConfig.getDistributedSuccessCache()));

    setupSerializers(jobConf);

    setJobName(jobConf, segments);

    if (converterConfig.getJobPriority() != null) {
      jobConf.setJobPriority(JobPriority.valueOf(converterConfig.getJobPriority()));
    }

    final Job job = Job.getInstance(jobConf);

    job.setInputFormatClass(ConfigInputFormat.class);
    job.setMapperClass(ConvertingMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setMapSpeculativeExecution(false);
    job.setOutputFormatClass(ConvertingOutputFormat.class);

    setupClassPath(job, jobConf);

    try {
      job.submit();
      log.info("Job %s submitted, status available at %s", job.getJobName(), job.getTrackingURL());
      final boolean success = job.waitForCompletion(true);
      if (!success) {
        final TaskReport[] reports = job.getTaskReports(TaskType.MAP);
        if (reports != null) {
          for (final TaskReport report : reports) {
            log.error("Error in task [%s] : %s", report.getTaskId(), Arrays.toString(report.getDiagnostics()));
          }
        }
        cleanup(job);
        return null;
      }
      try {
        loadedBytes = job.getCounters().findCounter(COUNTER_GROUP, COUNTER_LOADED).getValue();
        writtenBytes = job.getCounters().findCounter(COUNTER_GROUP, COUNTER_WRITTEN).getValue();
      }
      catch (IOException ex) {
        log.error(ex, "Could not fetch counters");
      }
      final JobID jobID = job.getJobID();

      final Path jobDir = getJobPath(jobID, job.getWorkingDirectory());
      final FileSystem fs = jobDir.getFileSystem(job.getConfiguration());
      final RemoteIterator<LocatedFileStatus> it = fs.listFiles(jobDir, true);
      final List<Path> goodPaths = new ArrayList<>();
      while (it.hasNext()) {
        final LocatedFileStatus locatedFileStatus = it.next();
        if (locatedFileStatus.isFile()) {
          final Path myPath = locatedFileStatus.getPath();
          if (ConvertingOutputFormat.DATA_SUCCESS_KEY.equals(myPath.getName())) {
            goodPaths.add(new Path(myPath.getParent(), ConvertingOutputFormat.DATA_FILE_KEY));
          }
        }
      }
      if (goodPaths.isEmpty()) {
        log.warn("No good data found at [%s]", jobDir);
        return null;
      }
      final List<DataSegment> returnList = ImmutableList.copyOf(
          Lists.transform(
              goodPaths, new Function<Path, DataSegment>()
              {
                @Nullable
                @Override
                public DataSegment apply(final Path input)
                {
                  try {
                    if (!fs.exists(input)) {
                      throw new ISE(
                          String.format(
                              "Somehow [%s] was found but [%s] is missing at [%s]",
                              ConvertingOutputFormat.DATA_SUCCESS_KEY,
                              ConvertingOutputFormat.DATA_FILE_KEY,
                              jobDir
                          )
                      );
                    }
                  }
                  catch (final IOException e) {
                    throw Throwables.propagate(e);
                  }
                  try (final InputStream stream = fs.open(input)) {
                    return HadoopDruidConverterConfig.jsonMapper.readValue(stream, DataSegment.class);
                  }
                  catch (final IOException e) {
                    throw Throwables.propagate(e);
                  }
                }
              }
          )
      );
      if (returnList.size() == segments.size()) {
        cleanup(job);
        return returnList;
      } else {
        throw new ISE(
            "Tasks reported success but result length did not match! Expected %d found %d at path [%s]",
            segments.size(),
            returnList.size(),
            jobDir
        );
      }
    }
    catch (InterruptedException | ClassNotFoundException e) {
      throw Throwables.propagate(e);
    }
  }

  public long getLoadedBytes()
  {
    return loadedBytes;
  }

  public long getWrittenBytes()
  {
    return writtenBytes;
  }

  private static void setupClassPath(Job job, JobConf jobConf)
      throws IOException
  {
    // Get the classpath stuff copied over

    String classpathProperty = System.getProperty("druid.hadoop.internal.classpath");
    if (classpathProperty == null) {
      classpathProperty = System.getProperty("java.class.path");
    }
    final String[] jarFiles = classpathProperty.split(File.pathSeparator);
    final Path distributedClassPath = new Path(jobConf.getWorkingDirectory(), "classpath");
    final FileSystem classpathFs = distributedClassPath.getFileSystem(job.getConfiguration());
    if (!(classpathFs instanceof LocalFileSystem)) {
      final Set<File> filesToCopy = new HashSet<>();
      for (String jarFilePath : jarFiles) {
        filesToCopy.addAll(Sets.newHashSet(findClassFiles(new File(jarFilePath))));
      }
      for (final File file : filesToCopy) {
        final Path distributedPath = new Path(distributedClassPath, file.getName());
        final boolean isSnapshot = file.getName().matches(".*SNAPSHOT(-selfcontained)?\\.jar$");
        final boolean isClass = file.getName().endsWith(".class");
        if (isClass || isSnapshot || !classpathFs.exists(distributedPath)) {
          final Path tmpFile = new Path(distributedClassPath, UUID.randomUUID().toString());
          final FileContext fileContext = FileContext.getFileContext(classpathFs.getUri());
          classpathFs.copyFromLocalFile(false, false, new Path(file.toURI()), tmpFile);
          if (isSnapshot || isClass) {
            fileContext.rename(tmpFile, distributedPath, Options.Rename.OVERWRITE);
          } else {
            try {
              fileContext.rename(tmpFile, distributedPath, Options.Rename.NONE);
            }
            catch (FileAlreadyExistsException ex) {
              if (!fileContext.delete(tmpFile, false)) {
                log.warn("Unable to delete temporary file [%s]", tmpFile);
              }
              log.warn(ex, "Lost a race on copying the file");
            }
          }
        }
        job.addFileToClassPath(distributedPath);
      }
      if (log.isDebugEnabled()) {
        log.debug("Added to classpath %s", Arrays.toString(job.getFileClassPaths()));
      }
    }
  }

  private static void setJobName(JobConf jobConf, List<DataSegment> segments)
  {
    if (segments.size() == 1) {
      final DataSegment segment = segments.get(0);
      jobConf.setJobName(
          String.format(
              "druid-convert-%s-%s-%s",
              segment.getDataSource(),
              segment.getInterval(),
              segment.getVersion()
          )
      );
    } else {
      final Set<String> segmentNames = Sets.newHashSet(
          Iterables.transform(
              segments,
              new Function<DataSegment, String>()
              {
                @Override
                public String apply(DataSegment input)
                {
                  return input.getDataSource();
                }
              }
          )
      );
      final Set<String> versions = Sets.newHashSet(
          Iterables.transform(
              segments,
              new Function<DataSegment, String>()
              {
                @Override
                public String apply(DataSegment input)
                {
                  return input.getVersion();
                }
              }
          )
      );
      jobConf.setJobName(
          String.format(
              "druid-convert-%s-%s",
              Arrays.toString(segmentNames.toArray()),
              Arrays.toString(versions.toArray())
          )
      );
    }
  }

  private static void setupSerializers(JobConf jobConf)
  {
    final Collection<String> serializers = jobConf.getStringCollection(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY);
    if (!serializers.contains(DataSegmentSplitSerializer.class.getName())) {
      serializers.add(DataSegmentSplitSerializer.class.getName());
    }
    jobConf.setStrings(
        CommonConfigurationKeys.IO_SERIALIZATIONS_KEY,
        serializers.toArray(new String[serializers.size()])
    );
  }

  public static Path getJobPath(JobID jobID, Path workingDirectory)
  {
    return new Path(workingDirectory, jobID.toString());
  }

  public static Path getTaskPath(JobID jobID, TaskAttemptID taskAttemptID, Path workingDirectory)
  {
    return new Path(getJobPath(jobID, workingDirectory), taskAttemptID.toString());
  }


  private static synchronized Injector getInjector(final Configuration configuration) throws IOException
  {
    if (injector == null) {
      final HadoopDruidConverterConfig config = converterConfigFromConfiguration(configuration);
      injector = GuiceInjectors.makeStartupInjector();
      injector = Initialization.makeInjectorWithModules(
          injector,
          ImmutableList.<Module>of(
              new Module()
              {
                @Override
                public void configure(Binder binder)
                {
                  JsonConfigProvider.bindInstance(
                      binder, Key.get(DruidNode.class, Self.class), new DruidNode("hadoop-converter", null, null)
                  );
                }
              }
          )
      );
    }
    return injector;
  }

  public static void cleanup(Job job) throws IOException
  {
    final Path jobDir = getJobPath(job.getJobID(), job.getWorkingDirectory());
    final FileSystem fs = jobDir.getFileSystem(job.getConfiguration());
    fs.delete(jobDir, true);
  }

  public static class ConvertingOutputFormat extends OutputFormat<Text, Text>
  {
    protected static final String DATA_FILE_KEY = "result";
    protected static final String DATA_SUCCESS_KEY = "_SUCCESS";
    protected static final String PUBLISHED_SEGMENT_KEY = "io.druid.indexer.updater.converter.publishedSegment";
    private static final Logger log = new Logger(ConvertingOutputFormat.class);

    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException
    {
      return new RecordWriter<Text, Text>()
      {
        @Override
        public void write(Text key, Text value) throws IOException, InterruptedException
        {
          // NOOP
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException
        {
          // NOOP
        }
      };
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException
    {
      // NOOP
    }

    @Override
    public OutputCommitter getOutputCommitter(final TaskAttemptContext context)
        throws IOException, InterruptedException
    {
      final MetadataStorageUpdaterJobHandler handler = getInjector(context.getConfiguration()).getInstance(
          MetadataStorageUpdaterJobHandler.class
      );

      return new OutputCommitter()
      {
        @Override
        public void setupJob(JobContext jobContext) throws IOException
        {

        }

        @Override
        public void setupTask(TaskAttemptContext taskContext) throws IOException
        {

        }

        @Override
        public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException
        {
          return taskContext.getConfiguration().get(PUBLISHED_SEGMENT_KEY) != null;
        }

        @Override
        public void commitTask(final TaskAttemptContext taskContext) throws IOException
        {
          final Progressable commitProgressable = new Progressable()
          {
            @Override
            public void progress()
            {
              taskContext.progress();
            }
          };
          final HadoopDruidConverterConfig config = converterConfigFromConfiguration(taskContext.getConfiguration());
          final String finalSegmentString = taskContext.getConfiguration().get(PUBLISHED_SEGMENT_KEY);
          if (finalSegmentString == null) {
            throw new IOException("Could not read final segment");
          }
          final DataSegment newSegment = HadoopDruidConverterConfig.jsonMapper.readValue(
              finalSegmentString,
              DataSegment.class
          );
          log.info("Committing new segment");
          taskContext.progress();

          final FileSystem fs = taskContext.getWorkingDirectory().getFileSystem(taskContext.getConfiguration());
          final Path taskAttemptDir = getTaskPath(
              context.getJobID(),
              context.getTaskAttemptID(),
              taskContext.getWorkingDirectory()
          );
          final Path taskAttemptFile = new Path(taskAttemptDir, DATA_FILE_KEY);
          final Path taskAttemptSuccess = new Path(taskAttemptDir, DATA_SUCCESS_KEY);
          try (final OutputStream outputStream = fs.create(taskAttemptFile, false, 1 << 10, commitProgressable)) {
            outputStream.write(HadoopDruidConverterConfig.jsonMapper.writeValueAsBytes(newSegment));
          }

          fs.create(taskAttemptSuccess, false).close();

          taskContext.progress();
          taskContext.setStatus("Committed");
        }

        @Override
        public void abortTask(TaskAttemptContext taskContext) throws IOException
        {
          log.warn("Aborting task");
        }
      };
    }
  }


  public static class ConvertingMapper extends Mapper<String, String, Text, Text>
  {
    private static final Logger log = new Logger(ConvertingMapper.class);
    private static final String TMP_FILE_LOC_KEY = "io.druid.indexer.updater.converter.reducer.tmpDir";

    @Override
    protected void map(
        String key, String value,
        final Context context
    ) throws IOException, InterruptedException
    {
      final InputSplit split = context.getInputSplit();
      if (!(split instanceof DataSegmentSplit)) {
        throw new IOException(
            String.format(
                "Unexpected split type. Expected [%s] was [%s]",
                DataSegmentSplit.class.getCanonicalName(),
                split.getClass().getCanonicalName()
            )
        );
      }

      final String tmpDirLoc = context.getConfiguration().get(TMP_FILE_LOC_KEY);
      final File tmpDir = Paths.get(tmpDirLoc).toFile();

      final DataSegment segment = ((DataSegmentSplit) split).getDataSegment();

      final HadoopDruidConverterConfig config = converterConfigFromConfiguration(context.getConfiguration());

      context.setStatus("DOWNLOADING");
      context.progress();
      final File inDir = new File(tmpDir, "in");
      if (inDir.exists() && !inDir.delete()) {
        log.warn("Could not delete [%s]", inDir);
      }
      if (!inDir.mkdir()) {
        log.warn("Unable to make directory");
      }
      final File tmpDownloadFile = File.createTempFile("dataSegment", ".zip");
      if (tmpDownloadFile.exists() && !tmpDownloadFile.delete()) {
        log.warn("Couldn't clear out temporary file [%s]", tmpDownloadFile);
      }
      try {
        final Path inPath = new Path(getURIFromSegment(segment));
        final FileSystem fs = inPath.getFileSystem(context.getConfiguration());
        if (fs instanceof LocalFileSystem) {
          java.nio.file.Files.copy(new File(inPath.toUri()).toPath(), tmpDownloadFile.toPath());
        } else {
          fs.copyToLocalFile(false, inPath, new Path(tmpDownloadFile.toURI()));
        }
        // This is to bypass Guava dependencies in CompressionUtils.unzip
        long size = 0L;
        try (final ZipInputStream zipInputStream = new ZipInputStream(
            new BufferedInputStream(
                new FileInputStream(
                    tmpDownloadFile
                )
            )
        )) {
          final byte[] buffer = new byte[1 << 13];
          for (ZipEntry entry = zipInputStream.getNextEntry(); entry != null; entry = zipInputStream.getNextEntry()) {
            final String fileName = entry.getName();
            try (final FileOutputStream fos = new FileOutputStream(
                inDir.getAbsolutePath()
                + File.separator
                + fileName
            )) {
              for (int len = zipInputStream.read(buffer); len >= 0; len = zipInputStream.read(buffer)) {
                size += len;
                fos.write(buffer, 0, len);
              }
            }
          }
        }
        log.debug("Loaded %d bytes into [%s] for converting", size, inDir.getAbsolutePath());
        context.getCounter(COUNTER_GROUP, COUNTER_LOADED)
               .increment(size);
      }
      finally {
        if (tmpDownloadFile.exists() && !tmpDownloadFile.delete()) {
          log.warn("Temporary download file could not be deleted [%s]", tmpDownloadFile);
        }
      }
      context.setStatus("CONVERTING");
      context.progress();
      final File outDir = new File(tmpDir, "out");
      if (!outDir.mkdir() && (!outDir.exists() || !outDir.isDirectory())) {
        throw new IOException(String.format("Could not create output directory [%s]", outDir));
      }
      IndexMaker.convert(
          inDir,
          outDir,
          config.getIndexSpec(),
          new ProgressIndicator()
          {
            @Override
            public void progress()
            {
              context.progress();
            }

            @Override
            public void start()
            {
              context.progress();
              context.setStatus("STARTED CONVERSION");
            }

            @Override
            public void stop()
            {
              context.progress();
              context.setStatus("STOPPED CONVERSION");
            }

            @Override
            public void startSection(String section)
            {
              context.progress();
              context.setStatus(String.format("STARTED [%s]", section));
            }

            @Override
            public void progressSection(String section, String message)
            {
              log.info("Progress message for section [%s] : [%s]", section, message);
              context.progress();
              context.setStatus(String.format("PROGRESS [%s]", section));
            }

            @Override
            public void stopSection(String section)
            {
              context.progress();
              context.setStatus(String.format("STOPPED [%s]", section));
            }
          }
      );
      final File[] files = outDir.listFiles();
      if (files == null) {
        throw new IOException(String.format("No files found in output directory! [%s]", outDir.getAbsolutePath()));
      }
      if (config.isValidate()) {
        context.setStatus("Validating");
        IndexIO.DefaultIndexIOHandler.validateTwoSegments(inDir, outDir);
      }
      context.progress();
      context.setStatus("Starting PUSH");
      long size = 0L;
      for (File file : files) {
        size += file.length();
      }
      final DataSegment finalSegmentOldLoadSpec = segment
          .withVersion(
              segment.getVersion()
              + "_converted"
          )
          .withSize(size)
          .withBinaryVersion(SegmentUtils.getVersionFromDir(outDir));

      // Make the zip file
      final DataSegment finalSegment;
      final File tmpZipFile = File.createTempFile("zipForUpload", ".zip");
      try {
        try (final ZipOutputStream zipOutputStream = new ZipOutputStream(
            new BufferedOutputStream(
                new FileOutputStream(
                    tmpZipFile
                )
            )
        )) {
          final byte[] buff = new byte[1 << 13];
          for (final File file : files) {
            final ZipEntry zipEntry = new ZipEntry(file.getName());
            zipOutputStream.putNextEntry(zipEntry);
            try (final InputStream fis = new BufferedInputStream(new FileInputStream(file))) {
              for (int read = fis.read(buff); read >= 0; read = fis.read(buff)) {
                zipOutputStream.write(buff, 0, read);
              }
            }
            context.progress();
          }
        }

        // Do the actual copy
        final Path outBaseSegmentPath = getOutputBasePathForSegment(
            finalSegmentOldLoadSpec,
            new Path(config.getSegmentOutputPath()),
            context.getConfiguration()
        );
        final FileSystem outFS = outBaseSegmentPath.getFileSystem(context.getConfiguration());
        final Path outIndexZipPath = new Path(outBaseSegmentPath, "index.zip");
        outFS.copyFromLocalFile(true, true, new Path(tmpZipFile.toURI()), outIndexZipPath);
        try (final OutputStream descriptorStream = outFS.create(
            new Path(outBaseSegmentPath, "descriptor.json"),
            true
        )) {
          final String outFSScheme = new Path(config.getSegmentOutputPath())
              .getFileSystem(context.getConfiguration())
              .getScheme();
          switch (outFSScheme) {
            case "file":
              finalSegment = finalSegmentOldLoadSpec.withLoadSpec(
                  ImmutableMap.<String, Object>of(
                      "type", "local",
                      "path", outIndexZipPath.toUri().getPath()
                  )
              );
              break;
            case "hdfs":
              finalSegment = finalSegmentOldLoadSpec.withLoadSpec(
                  ImmutableMap.<String, Object>of(
                      "type", "hdfs",
                      "path", outIndexZipPath.toUri().toString()
                  )
              );
              break;
            case "s3":
            case "s3n": // Should be `s3` but this is here just in case
              finalSegment = finalSegmentOldLoadSpec.withLoadSpec(
                  ImmutableMap.<String, Object>of(
                      "type", "s3_zip",
                      "bucket", outIndexZipPath.toUri().getHost(),
                      "key", outIndexZipPath.toUri().getPath().substring(1) // remove the leading "/"
                  )
              );
              break;
            default:
              throw new IAE("Unknown FS Scheme: [%s]", outFSScheme);
          }
          HadoopDruidConverterConfig.jsonMapper.writeValue(descriptorStream, finalSegment);
        }
      }
      finally {
        if (tmpZipFile.exists() && !tmpZipFile.delete()) {
          log.warn("Could not delete temporary file [%s]", tmpZipFile);
        }
      }

      context.progress();
      context.setStatus("Finished PUSH");
      final String finalSegmentString = HadoopDruidConverterConfig.jsonMapper.writeValueAsString(finalSegment);
      context.getConfiguration().set(ConvertingOutputFormat.PUBLISHED_SEGMENT_KEY, finalSegmentString);
      context.write(new Text("dataSegment"), new Text(finalSegmentString));

      for (File file : files) {
        context.getCounter(COUNTER_GROUP, COUNTER_WRITTEN).increment(file.length());
      }
      context.progress();
      context.setStatus("Ready To Commit");
    }

    // We must be consistent with `io.druid.indexer.HadoopDruidIndexerConfig.makeSegmentOutputPath()`
    private static Path getOutputBasePathForSegment(
        DataSegment segment,
        Path baseOutputPath,
        Configuration configuration
    )
        throws IOException
    {
      Path path = new Path(baseOutputPath, segment.getDataSource());
      if (path.getFileSystem(configuration) instanceof DistributedFileSystem) {
        path = new Path(
            path,
            String.format(
                "%s_%s",
                segment.getInterval().getStart().toString(ISODateTimeFormat.basicDateTime()),
                segment.getInterval().getEnd().toString(ISODateTimeFormat.basicDateTime())
            )
        );
        path = new Path(path, segment.getVersion().replace(':', '_'));
      } else {
        path = new Path(
            path,
            String.format(
                "./%s_%s",
                // Since time contains a `:` we specify this is a relative path or else the URI parser freaks out
                segment.getInterval().getStart().toString(),
                segment.getInterval().getEnd().toString()
            )
        );
        path = new Path(path, "./" + segment.getVersion());
      }
      return new Path(path, Integer.toString(segment.getShardSpec().getPartitionNum()));
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
      final File tmpFile = Files.createTempDir();
      context.getConfiguration().set(TMP_FILE_LOC_KEY, tmpFile.getAbsolutePath());
    }

    private static URI getURIFromSegment(DataSegment dataSegment)
    {
      // There is no good way around this...
      // TODO: add getURI() to URIDataPuller
      final Map<String, Object> loadSpec = dataSegment.getLoadSpec();
      final String type = loadSpec.get("type").toString();
      final URI segmentLocURI;
      if ("s3_zip".equals(type)) {
        segmentLocURI = URI.create(String.format("s3n://%s/%s", loadSpec.get("bucket"), loadSpec.get("key")));
      } else if ("hdfs".equals(type)) {
        segmentLocURI = URI.create(loadSpec.get("path").toString());
      } else if ("local".equals(type)) {
        try {
          segmentLocURI = new URI("file", null, loadSpec.get("path").toString(), null, null);
        }
        catch (URISyntaxException e) {
          throw new ISE(e, "Unable to form simple file uri");
        }
      } else {
        try {
          throw new IAE(
              "Cannot figure out loadSpec %s",
              HadoopDruidConverterConfig.jsonMapper.writeValueAsString(loadSpec)
          );
        }
        catch (JsonProcessingException e) {
          throw new ISE("Cannot write Map with json mapper");
        }
      }
      return segmentLocURI;
    }

    @Override
    protected void cleanup(
        Context context
    ) throws IOException, InterruptedException
    {
      final String tmpDirLoc = context.getConfiguration().get(TMP_FILE_LOC_KEY);
      final File tmpDir = Paths.get(tmpDirLoc).toFile();
      FileUtils.deleteDirectory(tmpDir);
      context.progress();
      context.setStatus("Clean");
    }
  }

  public static class DataSegmentSplitSerializer implements Serialization<DataSegmentSplit>
  {
    @Override
    public boolean accept(Class<?> c)
    {
      return DataSegmentSplit.class.isAssignableFrom(c);
    }

    @Override
    public Serializer<DataSegmentSplit> getSerializer(Class<DataSegmentSplit> c)
    {
      return new Serializer<DataSegmentSplit>()
      {
        OutputStream out;

        @Override
        public void open(OutputStream out) throws IOException
        {
          this.out = out;
        }

        @Override
        public void serialize(DataSegmentSplit dataSegmentSplit) throws IOException
        {
          out.write(HadoopDruidConverterConfig.jsonMapper.writeValueAsBytes(dataSegmentSplit.dataSegment));
        }

        @Override
        public void close() throws IOException
        {
          out.close();
        }
      };
    }

    @Override
    public Deserializer<DataSegmentSplit> getDeserializer(Class<DataSegmentSplit> c)
    {
      return new Deserializer<DataSegmentSplit>()
      {
        InputStream stream;

        @Override
        public void open(InputStream in) throws IOException
        {
          this.stream = in;
        }

        @Override
        public DataSegmentSplit deserialize(DataSegmentSplit dataSegmentSplit)
            throws IOException
        {
          return new DataSegmentSplit(HadoopDruidConverterConfig.jsonMapper.readValue(stream, DataSegment.class));
        }

        @Override
        public void close() throws IOException
        {
          stream.close();
        }
      };
    }
  }

  public static class DataSegmentSplit extends InputSplit
  {
    private final DataSegment dataSegment;

    public DataSegmentSplit(@NotNull DataSegment dataSegment)
    {
      this.dataSegment = Preconditions.checkNotNull(dataSegment, "dataSegment");
    }

    @Override
    public long getLength() throws IOException, InterruptedException
    {
      return dataSegment.getSize();
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException
    {
      return new String[]{};
    }

    protected DataSegment getDataSegment()
    {
      return dataSegment;
    }
  }

  public static class ConfigInputFormat extends InputFormat<String, String>
  {
    @Override
    public List<InputSplit> getSplits(final JobContext jobContext) throws IOException, InterruptedException
    {
      final HadoopDruidConverterConfig config = converterConfigFromConfiguration(jobContext.getConfiguration());
      final List<DataSegment> segments = config.getSegments();
      if (segments == null) {
        throw new IOException("Bad config, missing segments");
      }
      return Lists.transform(
          segments, new Function<DataSegment, InputSplit>()
          {
            @Nullable
            @Override
            public InputSplit apply(DataSegment input)
            {
              return new DataSegmentSplit(input);
            }
          }
      );
    }

    @Override
    public RecordReader<String, String> createRecordReader(
        final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext
    ) throws IOException, InterruptedException
    {
      return new RecordReader<String, String>()
      {
        boolean readAnything = false;

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException
        {

        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException
        {
          return !readAnything;
        }

        @Override
        public String getCurrentKey() throws IOException, InterruptedException
        {
          return "key";
        }

        @Override
        public String getCurrentValue() throws IOException, InterruptedException
        {
          readAnything = true;
          return "fakeValue";
        }

        @Override
        public float getProgress() throws IOException, InterruptedException
        {
          return readAnything ? 0.0F : 1.0F;
        }

        @Override
        public void close() throws IOException
        {
          // NOOP
        }
      };
    }
  }


  public static HadoopDruidConverterConfig converterConfigFromConfiguration(Configuration configuration)
      throws IOException
  {
    final String property = Preconditions.checkNotNull(
        configuration.get(HadoopDruidConverterConfig.CONFIG_PROPERTY),
        HadoopDruidConverterConfig.CONFIG_PROPERTY
    );
    return HadoopDruidConverterConfig.fromString(property);
  }

  public static void converterConfigIntoConfiguration(
      HadoopDruidConverterConfig priorConfig,
      List<DataSegment> segments,
      Configuration configuration
  )
  {
    final HadoopDruidConverterConfig config = new HadoopDruidConverterConfig(
        priorConfig.getDataSource(),
        priorConfig.getInterval(),
        priorConfig.getIndexSpec(),
        segments,
        priorConfig.isValidate(),
        priorConfig.getDistributedSuccessCache(),
        priorConfig.getHadoopProperties(),
        priorConfig.getJobPriority(),
        priorConfig.getSegmentOutputPath()
    );
    try {
      configuration.set(
          HadoopDruidConverterConfig.CONFIG_PROPERTY,
          HadoopDruidConverterConfig.jsonMapper.writeValueAsString(config)
      );
    }
    catch (JsonProcessingException e) {
      throw Throwables.propagate(e);
    }
  }

}
