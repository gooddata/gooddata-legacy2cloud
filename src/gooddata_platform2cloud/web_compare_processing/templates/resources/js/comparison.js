// (C) 2026 GoodData Corporation
// Show side-by-side comparison
function showComparison(rowId, platformUrl, cloudUrl, title) {
    // Update comparison title
    document.getElementById('comparisonTitle').textContent = 'Comparing: ' + title;

    // Store current row ID globally
    currentComparisonRowId = rowId;

    // Extract non-embedded URLs for the "Open in" buttons
    const row = document.getElementById(rowId);
    let platformNonEmbeddedUrl = '';
    let cloudNonEmbeddedUrl = '';

    if (row) {
        // Find the platform and cloud ID links (non-embedded URLs)
        const platformLink = row.querySelector('td:nth-child(4) a');
        const cloudLink = row.querySelector('td:nth-child(5) a');

        if (platformLink) {
            platformNonEmbeddedUrl = platformLink.href;
        }

        if (cloudLink) {
            cloudNonEmbeddedUrl = cloudLink.href;
        }
    }

    // Set up the "Open in Platform" button
    const openplatformButton = document.getElementById('openplatformButton');
    if (platformNonEmbeddedUrl) {
        openplatformButton.onclick = function() { window.open(platformNonEmbeddedUrl, '_blank'); };
        openplatformButton.style.display = 'inline-flex'; // Make sure it's visible
        openplatformButton.classList.remove('hidden');
    } else {
        openplatformButton.style.display = 'none';
        openplatformButton.classList.add('hidden');
    }

    // Set up the "Open in Cloud" button
    const opencloudButton = document.getElementById('opencloudButton');
    if (cloudNonEmbeddedUrl) {
        opencloudButton.onclick = function() { window.open(cloudNonEmbeddedUrl, '_blank'); };
        opencloudButton.style.display = 'inline-flex'; // Make sure it's visible
        opencloudButton.classList.remove('hidden');
    } else {
        opencloudButton.style.display = 'none';
        opencloudButton.classList.add('hidden');
    }

    const wasAlreadyOpen = isComparisonViewOpen;

    // If not already open, show the comparison container and set default sizes
    if (!wasAlreadyOpen) {
        // Show comparison header
        document.getElementById('comparisonHeader').classList.add('active');

        // Adjust table container height
        document.getElementById('tableContainer').classList.add('with-comparison');

        // Show comparison container
        document.getElementById('comparisonContainer').classList.add('active');

        // Hide summary cards
        document.getElementById('summaryCards').classList.add('hidden');

        // Make header compact - animations work because we've defined transitions
        document.getElementById('pageHeader').classList.add('compact');

        // Make main content compact to reduce padding
        document.querySelector('.main-content').classList.add('compact');

        // Set default container heights only when first opening
        document.getElementById('tableContainer').style.height = '18%';
        document.getElementById('comparisonContainer').style.height = '82%';

        // Collapse sidebar when first opening comparison view
        document.getElementById('sidebar').classList.add('collapsed');

        // Set flag that comparison is now open
        isComparisonViewOpen = true;
    }

    // Get iframe elements
    const platformFrame = document.getElementById('platformFrame');
    const cloudFrame = document.getElementById('cloudFrame');

    // Special handling for Platform frame reloading, to ensure reports are reloaded
    // First, detect if this is a report (reports have 'reportWidget.html' in the URL)
    const isReport = platformUrl.includes('reportWidget.html');

    if (isReport) {
        // For reports, we need to ensure the iframe is fully reloaded
        // First, set to blank and then set to the new URL after a short delay
        platformFrame.src = 'about:blank';
        setTimeout(() => {
            platformFrame.src = platformUrl;
        }, 50);
    } else {
        // Normal handling for other object types
        platformFrame.src = platformUrl;
    }

    // Set cloud iframe source
    cloudFrame.src = cloudUrl;

    // Highlight the current row
    highlightRow(rowId);

    // Set up navigation buttons for moving between items
    setupNavigationButtons(rowId);
}

// Set up navigation buttons for comparison view
function setupNavigationButtons(currentRowId) {
    // Get all comparable rows that are currently visible (not filtered out)
    const comparableRows = getComparableRows();

    // Find the index of the current row
    const currentIndex = comparableRows.findIndex(row => row.id === currentRowId);

    // If we can't find the current row or there are no comparable rows, disable both buttons
    if (currentIndex === -1 || comparableRows.length === 0) {
        const prevButton = document.getElementById('prevCompareButton');
        const nextButton = document.getElementById('nextCompareButton');
        prevButton.disabled = true;
        nextButton.disabled = true;
        return;
    }

    // Get button elements
    const prevButton = document.getElementById('prevCompareButton');
    const nextButton = document.getElementById('nextCompareButton');

    // Enable both buttons by default
    prevButton.disabled = false;
    nextButton.disabled = false;

    // Setup previous button
    if (currentIndex === 0) {
        // If on first row, show "To the Last" button pointing to the last visible row
        prevButton.textContent = "To the Last";
        prevButton.title = "Go to the last comparable item";
        prevButton.onclick = function() {
            const lastRow = comparableRows[comparableRows.length - 1];
            triggerCompareButtonClick(lastRow);
        };
    } else {
        // Normal previous button
        prevButton.textContent = "◀ Previous";
        prevButton.title = "Navigate to previous comparable item";
        prevButton.onclick = function() {
            const prevRow = comparableRows[currentIndex - 1];
            triggerCompareButtonClick(prevRow);
        };
    }

    // Setup next button
    if (currentIndex === comparableRows.length - 1) {
        // If on last row, show "To the First" button pointing to the first visible row
        nextButton.textContent = "To the First";
        nextButton.title = "Go to the first comparable item";
        nextButton.onclick = function() {
            const firstRow = comparableRows[0];
            triggerCompareButtonClick(firstRow);
        };
    } else {
        // Normal next button
        nextButton.textContent = "Next ▶";
        nextButton.title = "Navigate to next comparable item";
        nextButton.onclick = function() {
            const nextRow = comparableRows[currentIndex + 1];
            triggerCompareButtonClick(nextRow);
        };
    }
}

// Trigger click on a row's compare button
function triggerCompareButtonClick(row) {
    const compareBtn = row.querySelector('.compare-btn');
    if (compareBtn) {
        compareBtn.click();
    }
}

// Function to highlight a specific row
function highlightRow(rowId) {
    // Remove highlight from any previously highlighted row
    const highlightedRow = document.querySelector('.highlight-row');
    if (highlightedRow) {
        highlightedRow.classList.remove('highlight-row');
    }

    // Add highlight to the new row
    const row = document.getElementById(rowId);
    if (row) {
        row.classList.add('highlight-row');
        row.scrollIntoView({ behavior: 'smooth', block: 'center' });
    }
}

// Close comparison view
function closeComparison() {
    // Hide comparison header
    document.getElementById('comparisonHeader').classList.remove('active');

    // Reset table container height
    document.getElementById('tableContainer').classList.remove('with-comparison');

    // Hide comparison container
    document.getElementById('comparisonContainer').classList.remove('active');

    // Show summary cards
    document.getElementById('summaryCards').classList.remove('hidden');

    // Restore header to normal
    document.getElementById('pageHeader').classList.remove('compact');

    // Restore main content to normal
    document.querySelector('.main-content').classList.remove('compact');

    // Clear frame sources to stop any media/animations
    document.getElementById('platformFrame').src = 'about:blank';
    document.getElementById('cloudFrame').src = 'about:blank';

    // Reset flag
    isComparisonViewOpen = false;
    currentComparisonRowId = null;
}

// Set frame ratio based on percentages
function setFrameRatio(leftPercent, rightPercent) {
    const leftContainer = document.getElementById('leftFrameContainer');
    const rightContainer = document.getElementById('rightFrameContainer');
    const divider = document.getElementById('comparisonDivider');

    // Ensure divider stays visible
    if (leftPercent === 100) {
        leftContainer.style.flex = '0 0 calc(100% - 20px)'; // Increased from 6px to 20px
        rightContainer.style.flex = '0 0 0%';
        divider.style.width = '6px';
        // Move the divider away from the edge to keep controls visible
        divider.style.marginRight = '14px'; // Added margin to ensure controls are visible
        divider.style.marginLeft = '0';

        // Hide right container completely
        rightContainer.style.display = 'none';
        // Ensure left container is visible
        leftContainer.style.display = 'flex';
    } else if (rightPercent === 100) {
        leftContainer.style.flex = '0 0 0%';
        rightContainer.style.flex = '0 0 calc(100% - 20px)'; // Increased from 6px to 20px
        divider.style.width = '6px';
        // Move the divider away from the edge to keep controls visible
        divider.style.marginLeft = '14px'; // Added margin to ensure controls are visible
        divider.style.marginRight = '0';

        // Hide left container completely
        leftContainer.style.display = 'none';
        // Ensure right container is visible
        rightContainer.style.display = 'flex';
    } else {
        leftContainer.style.flex = `0 0 ${leftPercent}%`;
        rightContainer.style.flex = `0 0 ${rightPercent}%`;
        divider.style.marginLeft = '0';
        divider.style.marginRight = '0';

        // Make both containers visible
        leftContainer.style.display = 'flex';
        rightContainer.style.display = 'flex';
    }
}
