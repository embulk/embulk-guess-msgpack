/*
 * Copyright 2022 The Embulk project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.embulk.guess.msgpack;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.Comparator;
import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.embulk.spi.Exec;
import org.embulk.spi.type.Types;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.MessageInsufficientBufferException;
import org.msgpack.core.buffer.MessageBuffer;
import org.msgpack.core.buffer.MessageBufferInput;
import org.msgpack.value.Value;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.spi.Buffer;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.FileInput;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.BooleanType;
import org.embulk.spi.type.LongType;
import org.embulk.spi.type.DoubleType;
import org.embulk.spi.type.StringType;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.type.JsonType;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.config.units.ColumnConfig;
import org.embulk.util.config.units.SchemaConfig;
import org.embulk.util.timestamp.TimestampFormatter;

public class MsgpackGuessPlugin implements GuessPlugin
{
    @Override
    public ConfigDiff guess(final ConfigSource config, final Buffer sample) {
        final ConfigSource parserConfig = config.getNestedOrGetEmpty("parser");

        final ConfigDiff configDiff = CONFIG_MAPPER_FACTORY.newConfigDiff();

        // return {} unless config.fetch("parser", {}).fetch("type", "msgpack") == "msgpack"
        if (!"msgpack".equals(parserConfig.get(String.class, "type", "msgpack"))) {
            return configDiff;
        }

        /*
        classpath = File.expand_path('../../../../classpath', __FILE__)
        Dir["#{classpath}/*.jar"].each {|jar| require jar }
        */


        // If "charset" is not set yet, return only with a charset guessed.
        if (!parserConfig.has("charset")) {
            return CharsetGuess.of(CONFIG_MAPPER_FACTORY).guess(sample);
        }

        // If "newline" is not set yet, return only with a newline guessed.
        if (!parserConfig.has("newline")) {
            return NewlineGuess.of(CONFIG_MAPPER_FACTORY).guess(config, sample);
        }

        final BufferAllocator bufferAllocator = Exec.getBufferAllocator();
        return this.guessLines(config, LineGuessHelper.of(CONFIG_MAPPER_FACTORY).toLines(config, sample), bufferAllocator);

    }
    /*



        file_encoding = parser_config["file_encoding"]
        row_encoding = parser_config["row_encoding"]

        if !file_encoding || !row_encoding
          uk = new_unpacker(sample_buffer)
          begin
            n = uk.unpackArrayHeader
            begin
              n = uk.unpackArrayHeader
              file_encoding = "array"
              row_encoding = "array"
            rescue org.msgpack.core.MessageTypeException
              file_encoding = "sequence"
              row_encoding = "array"
            end
          rescue org.msgpack.core.MessageTypeException
            uk = new_unpacker(sample_buffer)  # TODO unpackArrayHeader consumes buffer (unexpectedly)
            begin
              n = uk.unpackMapHeader
              file_encoding = "sequence"
              row_encoding = "map"
            rescue org.msgpack.core.MessageTypeException
              return {}  # not a msgpack
            end
          end
        end

        uk = new_unpacker(sample_buffer)

        case file_encoding
        when "array"
          uk.unpackArrayHeader  # skip array header to convert to sequence
        when "sequence"
          # do nothing
        end

        rows = []

        begin
          while true
            rows << JSON.parse(uk.unpackValue.toJson)
          end
        rescue java.io.EOFException
        end

        if rows.size <= 3
          return {}
        end

        case row_encoding
        when "map"
          schema = Embulk::Guess::SchemaGuess.from_hash_records(rows)
        when "array"
          column_count = rows.map {|r| r.size }.max
          column_names = column_count.times.map {|i| "c#{i}" }
          schema = Embulk::Guess::SchemaGuess.from_array_records(column_names, rows)
        end

        parser_guessed = {"type" => "msgpack"}
        parser_guessed["row_encoding"] = row_encoding
        parser_guessed["file_encoding"] = file_encoding
        parser_guessed["columns"] = schema

        return {"parser" => parser_guessed}

      rescue org.msgpack.core.MessagePackException
        return {}
      end

      def new_unpacker(sample_buffer)
        org.msgpack.core.MessagePack.newDefaultUnpacker(sample_buffer.to_java_bytes)
      end
    */

    private static org.msgpack.core.MessagePack
      def new_unpacker(sample_buffer)
        org.msgpack.core.MessagePack.newDefaultUnpacker(sample_buffer.to_java_bytes)
      end

    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules().build();

    private static final Logger logger = LoggerFactory.getLogger(MsgpackGuessPlugin.class);
}
