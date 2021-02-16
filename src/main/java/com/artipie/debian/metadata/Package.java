/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2020 Artipie
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NON-INFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.artipie.debian.metadata;

import com.artipie.asto.Content;
import com.artipie.asto.Key;
import com.artipie.asto.Storage;
import com.artipie.debian.misc.PublisherAsArchive;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;

/**
 * Package index.
 * @since 0.1
 */
public interface Package {

    /**
     * Adds item to the packages index.
     * @param item Index item to add
     * @param index Package index key
     * @return Completion action
     */
    CompletionStage<Void> add(String item, Key index);

    /**
     * Simple {@link Package} implementation: it appends item to the index without any validation.
     * @since 0.1
     * @checkstyle ClassDataAbstractionCouplingCheck (100 lines)
     */
    final class Asto implements Package {

        /**
         * Abstract storage.
         */
        private final Storage asto;

        /**
         * Repository Release index.
         */
        private final Release release;

        /**
         * Ctor.
         * @param asto Storage
         * @param release Repository Release index
         */
        public Asto(final Storage asto, final Release release) {
            this.asto = asto;
            this.release = release;
        }

        @Override
        public CompletionStage<Void> add(final String item, final Key index) {
            return this.asto.exists(index).thenCompose(
                exists -> {
                    final CompletionStage<byte[]> res;
                    if (exists) {
                        res = this.asto.value(index).thenCompose(
                            content -> new PublisherAsArchive(content).unpackedGz()
                        ).thenApply(
                            bytes -> Asto.compress(
                                bytes,
                                "\n\n".getBytes(StandardCharsets.UTF_8),
                                item.getBytes(StandardCharsets.UTF_8)
                            )
                        );
                    } else {
                        res = CompletableFuture.completedFuture(
                            Asto.compress(item.getBytes(StandardCharsets.UTF_8))
                        );
                    }
                    return res;
                }
            )
                .thenCompose(res -> this.asto.save(index, new Content.From(res)))
                .thenCompose(nothing -> this.release.update(index));
        }

        /**
         * Compress bytes in gz format.
         * @param bytes Bytes to pack
         * @return Compressed bytes
         */
        private static byte[] compress(final byte[]... bytes) {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (GzipCompressorOutputStream gcos =
                new GzipCompressorOutputStream(new BufferedOutputStream(baos))) {
                for (final byte[] item : bytes) {
                    gcos.write(item);
                }
            } catch (final IOException ex) {
                throw new UncheckedIOException(ex);
            }
            return baos.toByteArray();
        }

    }

}
