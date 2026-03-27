// (C) 2026 GoodData Corporation
// Sidebar toggle functionality
document.addEventListener('DOMContentLoaded', function() {
    const sidebar = document.getElementById('sidebar');
    const sidebarToggle = document.getElementById('sidebarToggle');

    // Function to set the edge hover state
    function setEdgeHoverState(isHovered) {
        if (isHovered) {
            // Highlight the edge - set the color without transparency
            sidebar.style.setProperty('--edge-color', 'var(--sidebar-border-selected)');

            // Highlight the toggle icon
            sidebarToggle.classList.add('toggle-hovered');
        } else {
            // Remove highlighting
            sidebar.style.setProperty('--edge-color', 'transparent');

            // Remove toggle icon highlighting
            sidebarToggle.classList.remove('toggle-hovered');
        }
    }

    // Variable to track if we're hovering the toggle or edge
    let isHoveringToggle = false;
    let isHoveringEdge = false;

    // Add click handler to the toggle button
    sidebarToggle.addEventListener('click', function(e) {
        e.stopPropagation(); // Prevent event from bubbling up
        toggleSidebar();
    });

    // Add hover handlers to the toggle button
    sidebarToggle.addEventListener('mouseenter', function() {
        isHoveringToggle = true;
        setEdgeHoverState(true);
    });

    sidebarToggle.addEventListener('mouseleave', function() {
        isHoveringToggle = false;
        // Only remove highlight if we're not hovering the edge
        if (!isHoveringEdge) {
            setEdgeHoverState(false);
        }
    });

    // Handle edge hover effect
    sidebar.addEventListener('mousemove', function(e) {
        // Get position of mouse relative to sidebar
        const rect = sidebar.getBoundingClientRect();
        const posX = e.clientX - rect.left;

        // Define the edge width based on collapsed state
        const edgeWidth = sidebar.classList.contains('collapsed') ? 16 : 12;

        // Check if mouse is over the edge
        if (posX >= rect.width - edgeWidth) {
            isHoveringEdge = true;
            setEdgeHoverState(true);
        } else {
            isHoveringEdge = false;
            // Only remove highlight if we're not hovering the toggle
            if (!isHoveringToggle) {
                setEdgeHoverState(false);
            }
        }
    });

    // Remove hover effect when leaving the sidebar
    sidebar.addEventListener('mouseleave', function() {
        isHoveringEdge = false;
        // Only remove highlight if we're not hovering the toggle
        if (!isHoveringToggle) {
            setEdgeHoverState(false);
        }
    });

    // Make the sidebar edge clickable
    sidebar.addEventListener('click', function(e) {
        // Get position of click relative to sidebar
        const rect = sidebar.getBoundingClientRect();
        const clickX = e.clientX - rect.left;

        // Use a larger click area when collapsed for easier expansion
        const edgeWidth = sidebar.classList.contains('collapsed') ? 16 : 12;

        if (clickX >= rect.width - edgeWidth) {
            toggleSidebar();
        }
    });

    // Toggle sidebar function
    function toggleSidebar() {
        sidebar.classList.toggle('collapsed');

        // If comparison view is open, trigger resize event after transition completes
        if (isComparisonViewOpen) {
            // Trigger resize event after transition completes
            setTimeout(() => {
                window.dispatchEvent(new Event('resize'));
            }, 300);
        }
    }
});
