const socket = io();
var lastScrolledTo = null
socket.on('update', function(data) {
    const contentElement = document.getElementById('content');
    morphdom(contentElement, data, {childrenOnly: true});
    
    let scrollTo = document.querySelector('.scroll-to')
    if (scrollTo && scrollTo !== lastScrolledTo) {
	console.log('scroll', scrollTo)
    	scrollTo.scrollIntoView({behavior: 'smooth'});
	lastScrolledTo = scrollTo;
    }
});

let focusedCellId = null;

function runCell(cellId) {
    saveCode(cellId)
    socket.emit('run_cell', {cell_id: cellId});
}

function saveCode(cellId) {
    const codeContent = document.getElementById(`code_${cellId}`).content;
    socket.emit('save_code', {cell_id: cellId, content: codeContent});
}

socket.on('code_saved', function(data) {
    console.log(`Code saved for cell ${data.cell_id}`);
});

document.addEventListener('keydown', function(event) {
    console.log(event.key);
    
    function getKeyCombo(event) {
        let combo = [];
        if (event.ctrlKey) combo.push('Ctrl');
        if (event.altKey) combo.push('Alt');
        combo.push(event.key);
        return combo.join('+');
    }

    const keyCombo = getKeyCombo(event);
    console.log(keyCombo);
    socket.emit('keydown', { key: keyCombo });

    if (event.target === document.body)
        event.preventDefault();
});

socket.on('connect', function() {
    socket.emit('loaded', {});
})
