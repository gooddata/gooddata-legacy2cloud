// (C) 2026 GoodData Corporation
// Implement resizable divider
document.addEventListener('DOMContentLoaded', function() {
    const divider = document.getElementById('comparisonDivider');
    const framesContainer = document.getElementById('framesContainer');
    const leftContainer = document.getElementById('leftFrameContainer');
    const rightContainer = document.getElementById('rightFrameContainer');
    const tableContainer = document.getElementById('tableContainer');
    const comparisonContainer = document.getElementById('comparisonContainer');
    const verticalHandle = document.getElementById('verticalResizeHandle');
    const dragOverlay = document.getElementById('dragOverlay');

    let isDraggingHorizontal = false;
    let isDraggingVertical = false;
    let startX = 0;
    let startY = 0;
    let startLeftWidth = 0;
    let startTableHeight = 0;

    // Initialize global dragging system
    function initDragSystem() {
        // HORIZONTAL RESIZE - Divider between frames
        divider.addEventListener('mousedown', startHorizontalDrag);

        // VERTICAL RESIZE - Handle between table and comparison
        verticalHandle.addEventListener('mousedown', startVerticalDrag);
    }

    // Start horizontal drag
    function startHorizontalDrag(e) {
        // Prevent click on divider controls from triggering resize
        if (e.target.classList.contains('divider-button')) {
            return;
        }

        e.preventDefault();
        e.stopPropagation();

        // Ensure both containers are visible when dragging starts
        leftContainer.style.display = 'flex';
        rightContainer.style.display = 'flex';

        // Activate overlay to capture all mouse events
        dragOverlay.classList.add('active');

        isDraggingHorizontal = true;
        startX = e.clientX;

        const leftRect = leftContainer.getBoundingClientRect();
        startLeftWidth = leftRect.width;

        divider.classList.add('resizing');
        document.body.classList.add('resizing');

        // Disable iframe pointer events during drag
        disableIframeInteractions();

        // Set up overlay event handlers
        dragOverlay.onmousemove = handleDrag;
        dragOverlay.onmouseup = stopDrag;
        dragOverlay.onmouseleave = stopDrag;
    }

    // Start vertical drag
    function startVerticalDrag(e) {
        e.preventDefault();
        e.stopPropagation();

        // Activate overlay to capture all mouse events
        dragOverlay.classList.add('active');

        isDraggingVertical = true;
        startY = e.clientY;

        const tableRect = tableContainer.getBoundingClientRect();
        startTableHeight = tableRect.height;

        // Disable all transitions during drag
        tableContainer.classList.add('dragging');
        comparisonContainer.classList.add('dragging');

        verticalHandle.classList.add('resizing');
        document.body.classList.add('resizing-vertical');

        // Remove responsive class
        tableContainer.classList.remove('with-comparison');

        // Disable iframe pointer events during drag
        disableIframeInteractions();

        // Set up overlay event handlers
        dragOverlay.onmousemove = handleDrag;
        dragOverlay.onmouseup = stopDrag;
        dragOverlay.onmouseleave = stopDrag;
    }

    // Handle any active drag
    function handleDrag(e) {
        if (isDraggingHorizontal) {
            handleHorizontalDrag(e);
        } else if (isDraggingVertical) {
            handleVerticalDrag(e);
        }
    }

    // Handle horizontal dragging
    function handleHorizontalDrag(e) {
        // Calculate the move distance
        const deltaX = e.clientX - startX;

        // Calculate new width
        const containerWidth = framesContainer.getBoundingClientRect().width;
        const newLeftWidth = Math.max(containerWidth * 0.01, Math.min(startLeftWidth + deltaX, containerWidth * 0.99));

        // Convert to percentage and apply
        const leftPercentage = (newLeftWidth / containerWidth) * 100;
        const rightPercentage = 100 - leftPercentage;

        // Add magnetic "snap" effect on both edges
        const snapThreshold = 5; // Percentage threshold for the magnetic effect

        if (leftPercentage < snapThreshold) {
            // Left side magnetic effect - snap to 0%
            setFrameRatio(0, 100);
        } else if (rightPercentage < snapThreshold) {
            // Right side magnetic effect - snap to 100%
            setFrameRatio(100, 0);
        } else {
            // Normal resizing - apply calculated percentages
            setFrameRatio(leftPercentage, rightPercentage);
        }
    }

    // Handle vertical dragging
    function handleVerticalDrag(e) {
        // Calculate the move distance
        const deltaY = e.clientY - startY;

        // Calculate new heights
        const windowHeight = window.innerHeight;
        const newTableHeight = Math.max(40, Math.min(startTableHeight + deltaY, windowHeight - 50));

        // Convert to percentage of window height and apply
        const tablePercentage = (newTableHeight / windowHeight) * 100;
        const comparisonPercentage = 100 - tablePercentage;

        // Apply new sizes
        tableContainer.style.height = tablePercentage + '%';
        comparisonContainer.style.height = comparisonPercentage + '%';
    }

    // Disable iframe interactions during drag to prevent issues
    function disableIframeInteractions() {
        document.querySelectorAll('iframe').forEach(frame => {
            frame.style.pointerEvents = 'none';
        });
    }

    // Re-enable iframe interactions after drag ends
    function enableIframeInteractions() {
        document.querySelectorAll('iframe').forEach(frame => {
            frame.style.pointerEvents = 'auto';
        });
    }

    // Stop any active drag operation
    function stopDrag() {
        // Remove overlay
        dragOverlay.classList.remove('active');
        dragOverlay.onmousemove = null;
        dragOverlay.onmouseup = null;
        dragOverlay.onmouseleave = null;

        // Reset drag state
        isDraggingHorizontal = false;
        isDraggingVertical = false;

        // Remove resizing classes
        divider.classList.remove('resizing');
        verticalHandle.classList.remove('resizing');
        document.body.classList.remove('resizing');
        document.body.classList.remove('resizing-vertical');

        // Re-enable iframe interactions
        enableIframeInteractions();

        // Re-enable transitions
        tableContainer.classList.remove('dragging');
        comparisonContainer.classList.remove('dragging');
    }

    // Initialize the drag system
    initDragSystem();

    // Set up buttons for quick frame adjustments
    const leftOnlyBtn = document.getElementById('leftOnlyBtn');
    if (leftOnlyBtn) {
        leftOnlyBtn.addEventListener('click', function() {
            setFrameRatio(100, 0);
        });
    }

    const rightOnlyBtn = document.getElementById('rightOnlyBtn');
    if (rightOnlyBtn) {
        rightOnlyBtn.addEventListener('click', function() {
            setFrameRatio(0, 100);
        });
    }

    const splitEvenBtn = document.getElementById('splitEvenBtn');
    if (splitEvenBtn) {
        splitEvenBtn.addEventListener('click', function() {
            setFrameRatio(50, 50);
        });
    }

    // Set up search filtering
    const searchInput = document.getElementById('searchInput');
    if (searchInput) {
        searchInput.addEventListener('input', function() {
            // Store the current search term (trimmed and lowercase)
            currentSearchTerm = this.value.trim().toLowerCase();

            // Toggle clear button visibility
            toggleClearButton();

            // Apply the current filter with the new search term
            applyTableFilter(currentFilter);
        });

        // Clear button handler
        const clearButton = document.getElementById('searchClear');
        if (clearButton) {
            clearButton.addEventListener('click', function() {
                searchInput.value = '';
                currentSearchTerm = '';
                toggleClearButton();
                applyTableFilter(currentFilter);

                // Focus the search input after clearing
                searchInput.focus();
            });
        }

        // Initial clear button state
        function toggleClearButton() {
            const clearButton = document.getElementById('searchClear');
            if (clearButton) {
                clearButton.style.display = currentSearchTerm ? 'block' : 'none';
            }
        }

        // Set initial state
        toggleClearButton();
    }
});
