document.addEventListener('DOMContentLoaded', () => {
    const form = document.getElementById('lead-form');
    const startBtn = document.getElementById('startBtn');
    const btnText = startBtn.querySelector('.btn-text');
    const spinner = startBtn.querySelector('.loading-spinner');
    const statusPanel = document.getElementById('statusPanel');
    const successPanel = document.getElementById('successPanel');
    const progressFill = document.getElementById('progressFill');
    const statusMessage = document.getElementById('statusMessage');
    const timerElement = document.getElementById('timer');
    const terminalLogs = document.getElementById('terminalLogs');
    const toastStack = createToastStack();

    let timerInterval;
    let pollTimeoutId;
    let pollingStopped = false;
    let pollInFlight = false;
    let currentJobId = null;
    let lastPollErrorToastAt = 0;

    form.addEventListener('submit', async (e) => {
        e.preventDefault();

        const payload = {
            keyword: document.getElementById('keyword').value,
            minViews: parseInt(document.getElementById('minViews').value) || 0,
            maxViews: parseInt(document.getElementById('maxViews').value) || 0,
            minSubs: parseInt(document.getElementById('minSubs').value) || 0,
            maxSubs: parseInt(document.getElementById('maxSubs').value) || 0,
            region: document.getElementById('region').value,
            dateFilter: document.getElementById('dateFilter').value,
            searchPoolSize: parseInt(document.getElementById('searchPoolSize').value) || 500
        };

        stopPolling();
        clearInterval(timerInterval);
        lastPollErrorToastAt = 0;

        // UI: loading state
        btnText.textContent = "Processing Extraction...";
        spinner.classList.remove('hidden');
        startBtn.disabled = true;
        statusPanel.classList.remove('hidden');
        successPanel.classList.add('hidden');
        progressFill.style.width = '0%';
        terminalLogs.innerHTML = '<p>> Submitting job to server...</p>';
        startTimer();

        try {
            const res = await fetch('/api/extract', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
            });
            const data = await readResponseData(res);

            if (!res.ok) {
                throw new Error(getResponseErrorMessage(data, 'Failed to start extraction'));
            }

            currentJobId = data.jobId;
            appendLog(`> Job started - ID: ${currentJobId}`);
            startPolling(currentJobId);

        } catch (err) {
            appendLog(`> [ERR] Error: ${err.message}`);
            showToast(err.message || 'Failed to start extraction.', 'error');
            resetButton();
        }
    });

    function startPolling(jobId) {
        let logOffset = 0;
        pollingStopped = false;
        pollInFlight = false;

        const scheduleNextPoll = (delayMs = 1500) => {
            if (pollingStopped) return;
            pollTimeoutId = setTimeout(pollOnce, delayMs);
        };

        const pollOnce = async () => {
            if (pollingStopped || pollInFlight) return;
            pollInFlight = true;
            try {
                const res = await fetch(`/api/status/${jobId}?logOffset=${logOffset}`);
                const data = await readResponseData(res);
                if (!res.ok) {
                    const retryAfter = parseInt(res.headers.get('Retry-After') || '0', 10);
                    const err = new Error(getResponseErrorMessage(data, 'Unable to fetch job status.'));
                    if (Number.isFinite(retryAfter) && retryAfter > 0) {
                        err.retryAfterMs = retryAfter * 1000;
                    }
                    throw err;
                }

                // Append new logs
                const newLogs = Array.isArray(data.logs) ? data.logs : [];
                newLogs.forEach(log => appendLog(`> ${log}`));
                if (typeof data.nextLogOffset === 'number') {
                    logOffset = data.nextLogOffset;
                } else {
                    logOffset += newLogs.length;
                }

                // Update status message with latest log
                if (newLogs.length > 0) {
                    statusMessage.textContent = newLogs[newLogs.length - 1];
                }

                // Update progress bar
                if (data.total > 0) {
                    const pct = Math.round((data.progress / data.total) * 100);
                    progressFill.style.width = `${pct}%`;
                }

                // Check for completion
                if (data.status === 'completed') {
                    stopPolling();
                    finishExtraction(data.videosSearched, data.total, data.emailsFound, jobId);
                } else if (data.status === 'failed') {
                    stopPolling();
                    handleJobFailure(getResponseErrorMessage(data, 'Extraction job failed.'));
                } else {
                    scheduleNextPoll(1500);
                }

            } catch (err) {
                // Network error during polling - keep trying
                console.error('Poll error:', err);
                const now = Date.now();
                if (now - lastPollErrorToastAt > 8000) {
                    showToast(err.message || 'Network issue while polling status. Retrying...', 'warning');
                    lastPollErrorToastAt = now;
                }
                const retryMs = Number.isFinite(err.retryAfterMs) ? err.retryAfterMs : 2500;
                scheduleNextPoll(retryMs);
            } finally {
                pollInFlight = false;
            }
        };

        scheduleNextPoll(0);
    }

    function stopPolling() {
        pollingStopped = true;
        if (pollTimeoutId) {
            clearTimeout(pollTimeoutId);
            pollTimeoutId = null;
        }
    }

    function startTimer() {
        let seconds = 0;
        timerElement.textContent = "00:00";
        timerInterval = setInterval(() => {
            seconds++;
            const m = String(Math.floor(seconds / 60)).padStart(2, '0');
            const s = String(seconds % 60).padStart(2, '0');
            timerElement.textContent = `${m}:${s}`;
        }, 1000);
    }

    function finishExtraction(videosSearched, channelsMatched, emailsFound, jobId) {
        clearInterval(timerInterval);
        progressFill.style.width = '100%';

        setTimeout(() => {
            statusPanel.classList.add('hidden');
            successPanel.classList.remove('hidden');
            document.getElementById('vidCount').textContent = videosSearched || 0;
            document.getElementById('matchCount').textContent = channelsMatched || 0;
            document.getElementById('emailCount').textContent = emailsFound || 0;
            resetButton();
        }, 500);

        // Wire download button - use fetch + blob for reliable filename
        const downloadBtn = document.getElementById('downloadBtn');
        downloadBtn.onclick = async () => {
            try {
                const res = await fetch(`/api/download/${jobId}`);
                if (!res.ok) throw new Error('Download failed');
                const blob = await res.blob();
                const cd = res.headers.get('Content-Disposition') || '';
                const match = cd.match(/filename="?([^"]+)"?/);
                const safeJobId = jobId.replace(/[^a-zA-Z0-9]/g, "").substring(0, 8);
                const fname = match ? match[1] : `YTLeads_${safeJobId}.xlsx`;
                const url = URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = fname;
                document.body.appendChild(a);
                a.click();
                document.body.removeChild(a);
                URL.revokeObjectURL(url);
            } catch (err) {
                console.error('Download error:', err);
                showToast('Download failed. Please try again.', 'error');
            }
        };
    }

    function handleJobFailure(message) {
        appendLog(`> [ERR] Job failed: ${message}`);
        showToast(message || 'Extraction job failed.', 'error');
        statusMessage.textContent = message || 'Extraction failed.';
        progressFill.style.width = '100%';
        progressFill.style.background = 'rgba(255, 82, 82, 0.72)';
        statusPanel.style.opacity = '0.82';
        statusPanel.style.filter = 'saturate(0.8)';

        setTimeout(() => {
            statusPanel.classList.add('hidden');
            resetButton();
            statusMessage.textContent = 'Connecting to proxy network...';
            progressFill.style.width = '0%';
            progressFill.style.background = '';
            statusPanel.style.opacity = '';
            statusPanel.style.filter = '';
        }, 600);
    }

    function resetButton() {
        stopPolling();
        clearInterval(timerInterval);
        btnText.textContent = "Initialize Extraction";
        spinner.classList.add('hidden');
        startBtn.disabled = false;
    }

    async function readResponseData(res) {
        const bodyText = await res.text();
        if (!bodyText) return {};

        try {
            return JSON.parse(bodyText);
        } catch {
            return { raw: bodyText };
        }
    }

    function getResponseErrorMessage(data, fallbackMessage) {
        if (!data) return fallbackMessage;
        if (typeof data === 'string') return data.trim().slice(0, 240) || fallbackMessage;
        if (typeof data.error === 'string' && data.error.trim()) return data.error.trim().slice(0, 240);
        if (typeof data.message === 'string' && data.message.trim()) return data.message.trim().slice(0, 240);
        if (typeof data.raw === 'string' && data.raw.trim()) return data.raw.trim().replace(/\s+/g, ' ').slice(0, 240);
        return fallbackMessage;
    }

    function appendLog(msg) {
        const p = document.createElement('p');
        p.textContent = msg;
        terminalLogs.appendChild(p);
        terminalLogs.scrollTop = terminalLogs.scrollHeight;
    }

    function createToastStack() {
        const stack = document.createElement('div');
        stack.className = 'toast-stack';
        stack.setAttribute('aria-live', 'polite');
        stack.setAttribute('aria-atomic', 'true');
        document.body.appendChild(stack);
        return stack;
    }

    function showToast(message, type = 'error') {
        if (!message) return;

        const toast = document.createElement('div');
        toast.className = `toast toast-${type}`;
        toast.textContent = message;
        toastStack.appendChild(toast);

        requestAnimationFrame(() => {
            toast.classList.add('is-visible');
        });

        if (toastStack.children.length > 4) {
            toastStack.removeChild(toastStack.firstElementChild);
        }

        setTimeout(() => {
            toast.classList.remove('is-visible');
            toast.classList.add('is-hiding');
            setTimeout(() => toast.remove(), 280);
        }, 3800);
    }
});
