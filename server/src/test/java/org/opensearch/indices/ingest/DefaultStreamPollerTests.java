/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.ingest;

import org.junit.After;
import org.junit.Before;
import org.opensearch.index.IngestionShardPointer;
import org.opensearch.index.engine.FakeIngestionSource;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DefaultStreamPollerTests extends OpenSearchTestCase {
    private DefaultStreamPoller poller;
    private FakeIngestionSource.FakeIngestionConsumer fakeConsumer;
    private MessageProcessor mockProcessor;
    private List<byte[]> messages;
    private Set<IngestionShardPointer> persistedPointers;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        messages = new ArrayList<>();;
        messages.add("{\"name\":\"bob\", \"age\": 24}".getBytes());
        messages.add("{\"name\":\"alice\", \"age\": 21}".getBytes());
        fakeConsumer = new FakeIngestionSource.FakeIngestionConsumer(messages, 0);
        mockProcessor = mock(MessageProcessor.class);
        persistedPointers = new HashSet<>();
        poller = new DefaultStreamPoller(
            new FakeIngestionSource.FakeIngestionShardPointer(0),
            persistedPointers,
            fakeConsumer,
            mockProcessor,
            StreamPoller.ResetState.NONE
        );
    }

    @After
    public void tearDown() throws Exception {
        if (!poller.isClosed()) {
            poller.close();
        }
        super.tearDown();
    }

    public void testPauseAndResume() throws InterruptedException {
        poller.pause();
        poller.start();
        Thread.sleep(100); // Allow some time for the poller to run
        assertEquals(DefaultStreamPoller.State.PAUSED, poller.getState());
        assertTrue(poller.isPaused());
        // no messages are processed
        verify(mockProcessor, never()).process(any(),any());

        poller.resume();
        Thread.sleep(100); // Allow some time for the poller to run
        assertFalse(poller.isPaused());
        // 2 messages are processed
        verify(mockProcessor, times(2)).process(any(),any());
    }

    public void testSkipProcessed()  throws InterruptedException {
        messages.add("{\"name\":\"cathy\", \"age\": 21}".getBytes());
        messages.add("{\"name\":\"danny\", \"age\": 31}".getBytes());
        persistedPointers.add(new FakeIngestionSource.FakeIngestionShardPointer(1));
        persistedPointers.add(new FakeIngestionSource.FakeIngestionShardPointer(2));
        poller = new DefaultStreamPoller(
            new FakeIngestionSource.FakeIngestionShardPointer(0),
            persistedPointers,
            fakeConsumer,
            mockProcessor,
            StreamPoller.ResetState.NONE
        );
        poller.start();
        Thread.sleep(200); // Allow some time for the poller to run
        // 2 messages are processed, 2 messages are skipped
        verify(mockProcessor, times(2)).process(any(),any());
        assertEquals(new FakeIngestionSource.FakeIngestionShardPointer(2), poller.getMaxPersistedPointer());
    }

    public void testCloseWithoutStart() {
        poller.close();
        assertTrue(poller.isClosed());
    }

    public void testClose() throws InterruptedException {
        poller.start();
        Thread.sleep(100); // Allow some time for the poller to run
        poller.close();
        assertTrue(poller.isClosed());
        assertEquals(DefaultStreamPoller.State.CLOSED, poller.getState());
    }


    public void testResetStateEarliest() throws InterruptedException {
        poller = new DefaultStreamPoller(
            new FakeIngestionSource.FakeIngestionShardPointer(1),
            persistedPointers,
            fakeConsumer,
            mockProcessor,
            StreamPoller.ResetState.EARLIEST
        );

        poller.start();
        Thread.sleep(100); // Allow some time for the poller to run

        // 2 messages are processed
        verify(mockProcessor, times(2)).process(any(),any());
    }

    public void testResetStateLatest() throws InterruptedException {
        poller = new DefaultStreamPoller(
            new FakeIngestionSource.FakeIngestionShardPointer(0),
            persistedPointers,
            fakeConsumer,
            mockProcessor,
            StreamPoller.ResetState.LATEST
        );

        poller.start();
        Thread.sleep(100); // Allow some time for the poller to run
        // no messages processed
        verify(mockProcessor, never()).process(any(),any());
        // reset to the latest
        assertEquals(new FakeIngestionSource.FakeIngestionShardPointer(2), poller.getBatchStartPointer());
    }

    public void testStartPollWithoutStart() {
        try {
            poller.startPoll();
            fail("Expected an exception to be thrown");
        } catch (IllegalStateException e) {
            assertEquals("poller is not started!", e.getMessage());
        }
    }

    public void testStartClosedPoller() throws InterruptedException {
        poller.start();
        Thread.sleep(100);
        poller.close();
        try {
            poller.startPoll();
            fail("Expected an exception to be thrown");
        } catch (IllegalStateException e) {
            assertEquals("poller is closed!", e.getMessage());
        }
    }
}
