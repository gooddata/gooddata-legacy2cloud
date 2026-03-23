// (C) 2026 GoodData Corporation
// Global state variables
let isComparisonViewOpen = false;
let currentComparisonRowId = null;
let currentFilter = 'all';
let currentSearchTerm = '';

// Initialize main functionality when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    // Get all summary cards and add click handlers
    const summaryCards = document.querySelectorAll('.summary-card');
    summaryCards.forEach(card => {
        card.addEventListener('click', function() {
            const filter = this.getAttribute('data-filter');

            // Toggle filter
            if (currentFilter === filter) {
                // Deselect
                currentFilter = 'all';
                summaryCards.forEach(c => {
                    c.classList.remove('selected');
                    c.classList.remove('selected-' + c.getAttribute('data-filter'));
                });
                document.getElementById('totalCard').classList.add('selected');
                document.getElementById('totalCard').classList.add('selected-success');

                // Apply 'all' filter to show all records
                applyTableFilter('all');
            } else {
                // Apply new filter
                currentFilter = filter;
                // Remove selection from all cards
                summaryCards.forEach(c => {
                    c.classList.remove('selected');
                    c.classList.remove('selected-' + c.getAttribute('data-filter'));
                });
                // Add selection to this card
                this.classList.add('selected');
                // Add specific status class for the colored indicator
                if (filter !== 'all') {
                    this.classList.add('selected-' + filter);
                } else {
                    this.classList.add('selected-success');
                }

                // Apply filter to table
                applyTableFilter(filter);
            }
        });
    });

    // Set initial selected state to "Total"
    document.getElementById('totalCard').classList.add('selected');
    document.getElementById('totalCard').classList.add('selected-success');

    // Set up sidebar navigation for report types and client prefixes
    setupSidebarNavigation();
});
