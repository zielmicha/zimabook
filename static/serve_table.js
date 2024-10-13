class DataTable extends HTMLElement {
    constructor() {
        super();
        this.attachShadow({mode: 'open'});
    }

    static get observedAttributes() {
        return ['server-url'];
    }

    connectedCallback() {
        //this.initializeTable();
    }

    attributeChangedCallback(name, oldValue, newValue) {
	console.log(name, oldValue, newValue)
        if (name === 'server-url' && oldValue !== newValue) {
	    this.shadowRoot.innerHTML = '';
	    this.initializeTable();
        }
    }

    initializeTable() {
        const serverUrl = this.getAttribute('server-url');
        //console.log("serverUrl", serverUrl);
        
        this.shadowRoot.innerHTML = `
            <style>
                @import url("/deps/jquery.dataTables.min.css");
            </style>
            <table id="data-table" class="display" style="width:100%">
                <thead>
                </thead>
            </table>
        `;

        $(this.shadowRoot).ready(() => {
            $.ajax({
                url: serverUrl + (serverUrl.indexOf('?') == -1 ? "?" : "&") + "get-columns=true",
                method: 'POST',
                success: (columns) => {
                    const dtColumns = columns.map(column => ({
                        data: column,
                        title: column
                    }));

                    $(this.shadowRoot.getElementById('data-table')).DataTable({
                        "processing": true,
                        "serverSide": true,
                        "ajax": {"url": serverUrl, "type": "POST"},
                        "columns": dtColumns,
                        "searchDelay": 500,
                    });
                },
                error: (xhr, status, error) => {
                    alert('Failed to fetch columns: ' + error);
                }
            });
        });
    }
}

customElements.define('data-table', DataTable);
