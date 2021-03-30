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
package com.artipie.debian.misc;

import com.artipie.asto.Content;
import com.artipie.asto.test.TestResource;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.hamcrest.MatcherAssert;
import org.hamcrest.core.IsEqual;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link RosUnpackedContent}.
 * @since 0.5
 */
class RosUnpackedContentTest {

    @Test
    void calcsSizeAndDigest() {
        MatcherAssert.assertThat(
            new RosUnpackedContent(
                new Content.From(new TestResource("Packages.gz").asBytes())
            ).sizeAndDigest().toCompletableFuture().join(),
            new IsEqual<>(
                new ImmutablePair<>(
                    // @checkstyle MagicNumberCheck (1 line)
                    2564L, "c1cfc96b4ca50645c57e10b65fcc89fd1b2b79eb495c9fa035613af7ff97dbff"
                )
            )
        );
    }

}
