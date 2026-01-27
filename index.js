/**
 * Chat Branches Server Plugin
 * High-performance branch relationship tracking using node-persist
 */

const path = require('path');
const fs = require('fs').promises;
const storage = require('node-persist');

let initialized = false;

/**
 * Initialize the plugin
 * @param {import('express').Router} router Express router
 * @returns {Promise<void>}
 */
async function init(router) {
    console.log('[Chat Branches] Initializing plugin...');

    // Initialize node-persist storage
    const dataDir = path.join(__dirname, 'data');
    await storage.init({
        dir: dataDir,
        stringify: JSON.stringify,
        parse: JSON.parse,
        encoding: 'utf8',
        logging: false,
        ttl: false
    });

    initialized = true;
    console.log('[Chat Branches] Storage initialized at:', dataDir);

    // Route: Delete all data for a character
    router.delete('/character/:characterId', async (req, res) => {
        try {
            const { characterId } = req.params;
            console.log('[Chat Branches] Deleting character data:', characterId);
            
            const deletedCount = await deleteCharacterData(characterId);
            
            res.json({
                success: true,
                message: `Deleted ${deletedCount} branches for character ${characterId}`
            });
        } catch (error) {
            console.error('[Chat Branches] Error deleting character data:', error);
            res.status(500).json({ success: false, error: error.message });
        }
    });

    // Route: Health check (HEAD request)
    router.head('/', async (req, res) => {
        res.status(200).end();
    });

    // Route: Register a new branch
    router.post('/branch', async (req, res) => {
        try {
            const { uuid, parent_uuid, root_uuid, character_id, chat_name, branch_point, created_at } = req.body;

            if (!uuid || !root_uuid) {
                return res.status(400).json({
                    success: false,
                    error: 'Missing required fields: uuid, root_uuid'
                });
            }

            // Check if branch already exists to prevent duplicates
            const existingBranch = await storage.getItem(`branch:${uuid}`);
            if (existingBranch) {
                console.log('[Chat Branches] Branch already exists, skipping registration:', uuid);
                return res.json({ success: true, message: 'Branch already exists' });
            }

            // Ensure chat_name doesn't have .jsonl extension (we store clean names)
            const cleanChatName = chat_name ? String(chat_name).replace(/\.jsonl$/i, '') : null;

            const branch = {
                uuid,
                parent_uuid: parent_uuid || null,
                root_uuid,
                character_id: character_id || null,
                chat_name: cleanChatName,
                branch_point: branch_point || null,
                created_at: created_at || Date.now()
            };

            // Store branch by UUID
            await storage.setItem(`branch:${uuid}`, branch);

            // Index by character for fast lookups
            if (character_id) {
                let charBranches = await storage.getItem(`char:${character_id}`) || [];
                if (!charBranches.includes(uuid)) {
                    charBranches.push(uuid);
                }
                // Deduplicate to prevent issues (ensure no duplicates even in race conditions)
                charBranches = [...new Set(charBranches)];
                await storage.setItem(`char:${character_id}`, charBranches);
            }

            // Index by root for fast tree queries
            let rootBranches = await storage.getItem(`root:${root_uuid}`) || [];
            if (!rootBranches.includes(uuid)) {
                rootBranches.push(uuid);
            }
            // Deduplicate to prevent issues (ensure no duplicates even in race conditions)
            rootBranches = [...new Set(rootBranches)];
            await storage.setItem(`root:${root_uuid}`, rootBranches);

            res.json({ success: true });
        } catch (error) {
            console.error('[Chat Branches] Error inserting branch:', error);
            res.status(500).json({ success: false, error: error.message });
        }
    });

    // Route: Get full tree for a character
    router.get('/tree/:characterId', async (req, res) => {
        try {
            const { characterId } = req.params;

            // Get all branch UUIDs for this character
            let branchUuids = await storage.getItem(`char:${characterId}`) || [];

            // Deduplicate UUIDs to prevent duplicate branches
            branchUuids = [...new Set(branchUuids)];

            // Fetch all branches
            const branches = [];
            for (const uuid of branchUuids) {
                const branch = await storage.getItem(`branch:${uuid}`);
                if (branch) branches.push(branch);
            }

            // Sort by creation date
            branches.sort((a, b) => a.created_at - b.created_at);

            // Build tree structure
            const tree = buildTree(branches);

            res.json({ success: true, tree });
        } catch (error) {
            console.error('[Chat Branches] Error fetching tree:', error);
            res.status(500).json({ success: false, error: error.message });
        }
    });

    // Route: Get tree for specific root
    router.get('/tree/root/:rootUuid', async (req, res) => {
        try {
            const { rootUuid } = req.params;

            // Get all branch UUIDs for this root
            const branchUuids = await storage.getItem(`root:${rootUuid}`) || [];
            
            // Fetch all branches
            const branches = [];
            for (const uuid of branchUuids) {
                const branch = await storage.getItem(`branch:${uuid}`);
                if (branch) branches.push(branch);
            }

            // Sort by creation date
            branches.sort((a, b) => a.created_at - b.created_at);

            // Build tree structure
            const tree = buildTree(branches);

            res.json({ success: true, tree });
        } catch (error) {
            console.error('[Chat Branches] Error fetching tree by root:', error);
            res.status(500).json({ success: false, error: error.message });
        }
    });

    // Route: Get children of a specific chat
    router.get('/children/:uuid', async (req, res) => {
        try {
            const { uuid } = req.params;

            // Get all branches for the parent's root
            const parent = await storage.getItem(`branch:${uuid}`);
            if (!parent) {
                return res.json({ success: true, children: [] });
            }

            const branchUuids = await storage.getItem(`root:${parent.root_uuid}`) || [];
            
            // Filter to only children of this UUID
            const children = [];
            for (const childUuid of branchUuids) {
                const branch = await storage.getItem(`branch:${childUuid}`);
                if (branch && branch.parent_uuid === uuid) {
                    children.push(branch);
                }
            }

            // Sort by creation date
            children.sort((a, b) => a.created_at - b.created_at);

            res.json({ success: true, children });
        } catch (error) {
            console.error('[Chat Branches] Error fetching children:', error);
            res.status(500).json({ success: false, error: error.message });
        }
    });

    // Route: Get branch info by UUID
    router.get('/branch/:uuid', async (req, res) => {
        try {
            const { uuid } = req.params;

            const branch = await storage.getItem(`branch:${uuid}`);

            if (!branch) {
                return res.status(404).json({ 
                    success: false, 
                    error: 'Branch not found' 
                });
            }

            res.json({ success: true, branch });
        } catch (error) {
            console.error('[Chat Branches] Error fetching branch:', error);
            res.status(500).json({ success: false, error: error.message });
        }
    });

    // Route: Delete a branch and optionally its children
    router.delete('/branch/:uuid', async (req, res) => {
        try {
            const { uuid } = req.params;
            const { cascade } = req.query; // ?cascade=true to delete children too

            const branch = await storage.getItem(`branch:${uuid}`);
            if (!branch) {
                return res.status(404).json({ 
                    success: false, 
                    error: 'Branch not found' 
                });
            }

            if (cascade === 'true') {
                // Delete recursively
                await deleteRecursive(uuid, branch);
            } else {
                // Just delete this one
                await deleteBranch(uuid, branch);
            }

            res.json({ success: true });
        } catch (error) {
            console.error('[Chat Branches] Error deleting branch:', error);
            res.status(500).json({ success: false, error: error.message });
        }
    });

    // Route: Update branch metadata (e.g., chat_name after rename)
    router.patch('/branch/:uuid', async (req, res) => {
        try {
            const { uuid } = req.params;
            const { chat_name, character_id, parent_uuid, root_uuid } = req.body;

            const branch = await storage.getItem(`branch:${uuid}`);
            if (!branch) {
                console.warn('[Chat Branches Plugin] Branch not found for UUID:', uuid);
                return res.status(404).json({
                    success: false,
                    error: 'Branch not found'
                });
            }

            let updated = false;

            if (chat_name !== undefined) {
                // Ensure chat_name doesn't have .jsonl extension (we store clean names)
                const cleanChatName = String(chat_name).replace(/\.jsonl$/i, '');
                branch.chat_name = cleanChatName;
                updated = true;
            }

            if (character_id !== undefined && character_id !== branch.character_id) {
                // Remove from old character index
                if (branch.character_id) {
                    const oldCharBranches = await storage.getItem(`char:${branch.character_id}`) || [];
                    const filtered = oldCharBranches.filter(id => id !== uuid);
                    await storage.setItem(`char:${branch.character_id}`, filtered);
                }

                // Add to new character index
                const newCharBranches = await storage.getItem(`char:${character_id}`) || [];
                if (!newCharBranches.includes(uuid)) {
                    newCharBranches.push(uuid);
                    await storage.setItem(`char:${character_id}`, newCharBranches);
                }

                branch.character_id = character_id;
                updated = true;
            }

            // Preserve parent_uuid if provided (prevents structure corruption)
            if (parent_uuid !== undefined && parent_uuid !== branch.parent_uuid) {
                // If moving between roots, update root indices
                if (branch.root_uuid !== root_uuid) {
                    // Remove from old root index
                    const oldRootBranches = await storage.getItem(`root:${branch.root_uuid}`) || [];
                    const filtered = oldRootBranches.filter(id => id !== uuid);
                    await storage.setItem(`root:${branch.root_uuid}`, filtered);
                    
                    // Add to new root index
                    const newRootBranches = await storage.getItem(`root:${root_uuid}`) || [];
                    if (!newRootBranches.includes(uuid)) {
                        newRootBranches.push(uuid);
                        await storage.setItem(`root:${root_uuid}`, newRootBranches);
                    }
                }
                
                branch.parent_uuid = parent_uuid;
                updated = true;
            }

            // Preserve root_uuid if provided (prevents structure corruption)
            if (root_uuid !== undefined && root_uuid !== branch.root_uuid) {
                // If root is changing, update root indices
                if (branch.root_uuid) {
                    const oldRootBranches = await storage.getItem(`root:${branch.root_uuid}`) || [];
                    const filtered = oldRootBranches.filter(id => id !== uuid);
                    await storage.setItem(`root:${branch.root_uuid}`, filtered);
                }
                
                // Add to new root index
                const newRootBranches = await storage.getItem(`root:${root_uuid}`) || [];
                if (!newRootBranches.includes(uuid)) {
                    newRootBranches.push(uuid);
                    await storage.setItem(`root:${root_uuid}`, newRootBranches);
                }
                
                branch.root_uuid = root_uuid;
                updated = true;
            }

            if (!updated) {
                return res.status(400).json({
                    success: false,
                    error: 'No fields to update'
                });
            }

            await storage.setItem(`branch:${uuid}`, branch);

            res.json({
                success: true,
                branch: branch
            });
        } catch (error) {
            console.error('[Chat Branches Plugin] Error updating branch:', error);
            res.status(500).json({ success: false, error: error.message });
        }
    });

    // Route: Get all orphaned branches (parent_uuid doesn't exist)
    router.get('/orphans/:characterId', async (req, res) => {
        try {
            const { characterId } = req.params;

            const branchUuids = await storage.getItem(`char:${characterId}`) || [];
            
            const orphans = [];
            for (const uuid of branchUuids) {
                const branch = await storage.getItem(`branch:${uuid}`);
                if (!branch || !branch.parent_uuid) continue;

                // Check if parent exists
                const parent = await storage.getItem(`branch:${branch.parent_uuid}`);
                if (!parent) {
                    orphans.push(branch);
                }
            }

            res.json({ success: true, orphans });
        } catch (error) {
            console.error('[Chat Branches] Error fetching orphans:', error);
            res.status(500).json({ success: false, error: error.message });
        }
    });

    // Route: Get all branches (for searching by chat_name)
    router.get('/branches', async (req, res) => {
        try {
            const { chat_name } = req.query;

            // Get all branch keys
            const keys = await storage.keys();
            const branchKeys = keys.filter(k => k.startsWith('branch:'));

            // Fetch all branches
            const branches = [];
            for (const key of branchKeys) {
                const branch = await storage.getItem(key);
                if (branch) {
                    // Filter by chat_name if provided
                    if (!chat_name || branch.chat_name === chat_name) {
                        branches.push(branch);
                    }
                }
            }

            // Sort by creation date
            branches.sort((a, b) => a.created_at - b.created_at);

            res.json({ success: true, branches });
        } catch (error) {
            console.error('[Chat Branches] Error fetching branches:', error);
            res.status(500).json({ success: false, error: error.message });
        }
    });

    // Route: Clean duplicates from storage
    router.post('/clean-duplicates', async (req, res) => {
        try {
            console.log('[Chat Branches] Starting duplicate cleanup...');

            const keys = await storage.keys();
            const charKeys = keys.filter(k => k.startsWith('char:'));
            const rootKeys = keys.filter(k => k.startsWith('root:'));

            let totalDuplicatesRemoved = 0;

            // Clean character indices
            for (const key of charKeys) {
                const original = await storage.getItem(key) || [];
                const deduplicated = [...new Set(original)];
                if (deduplicated.length !== original.length) {
                    const removed = original.length - deduplicated.length;
                    totalDuplicatesRemoved += removed;
                    console.log(`[Chat Branches] Cleaned ${removed} duplicates from ${key}`);
                    await storage.setItem(key, deduplicated);
                }
            }

            // Clean root indices
            for (const key of rootKeys) {
                const original = await storage.getItem(key) || [];
                const deduplicated = [...new Set(original)];
                if (deduplicated.length !== original.length) {
                    const removed = original.length - deduplicated.length;
                    totalDuplicatesRemoved += removed;
                    console.log(`[Chat Branches] Cleaned ${removed} duplicates from ${key}`);
                    await storage.setItem(key, deduplicated);
                }
            }

            res.json({
                success: true,
                message: `Cleaned ${totalDuplicatesRemoved} duplicate entries from storage`
            });
        } catch (error) {
            console.error('[Chat Branches] Error cleaning duplicates:', error);
            res.status(500).json({ success: false, error: error.message });
        }
    });

    // Route: Reset database (useful for testing)
    router.post('/reset', async (req, res) => {
        try {
            await storage.clear();
            res.json({ success: true, message: 'Database reset' });
        } catch (error) {
            console.error('[Chat Branches] Error resetting database:', error);
            res.status(500).json({ success: false, error: error.message });
        }
    });

    // Route: Get chat messages directly from file
    router.post('/messages/:uuid', async (req, res) => {
        try {
            const { uuid } = req.params;
            const { character_name } = req.body;

            // Get branch info to find the chat name
            const branch = await storage.getItem(`branch:${uuid}`);
            if (!branch) {
                return res.status(404).json({
                    success: false,
                    error: 'Branch not found'
                });
            }

            if (!branch.chat_name) {
                return res.status(404).json({
                    success: false,
                    error: 'Branch has no chat_name associated'
                });
            }

            // Construct the path to the chat file
            // SillyTavern stores chats in: /chats/{character_name}/{chat_name}.jsonl
            // Ensure we don't double-add .jsonl extension
            const cleanChatName = String(branch.chat_name).replace(/\.jsonl$/i, '');
            const chatFileName = `${cleanChatName}.jsonl`;
            const chatFilePath = path.join(process.cwd(), 'chats', character_name || branch.character_id || '', chatFileName);

            // Read the file
            const fileContent = await fs.readFile(chatFilePath, 'utf8');

            // Parse JSONL format
            const lines = fileContent.split('\n').filter(line => line.trim());
            const messages = [];

            for (const line of lines) {
                try {
                    const parsed = JSON.parse(line);
                    messages.push(parsed);
                } catch (parseError) {
                    console.warn('[Chat Branches] Failed to parse line:', parseError.message);
                    // Skip malformed lines
                }
            }

            res.json({
                success: true,
                messages: messages,
                chat_name: cleanChatName
            });
        } catch (error) {
            console.error('[Chat Branches] Error loading chat messages:', error);
            res.status(500).json({
                success: false,
                error: error.message
            });
        }
    });

    // Route: Get database stats
    router.get('/stats', async (req, res) => {
        try {
            const keys = await storage.keys();
            const branchKeys = keys.filter(k => k.startsWith('branch:'));
            const charKeys = keys.filter(k => k.startsWith('char:'));
            const rootKeys = keys.filter(k => k.startsWith('root:'));

            res.json({
                success: true,
                stats: {
                    totalBranches: branchKeys.length,
                    characters: charKeys.length,
                    roots: rootKeys.length
                }
            });
        } catch (error) {
            console.error('[Chat Branches] Error fetching stats:', error);
            res.status(500).json({ success: false, error: error.message });
        }
    });

    console.log('[Chat Branches] Plugin initialized successfully');
}

/**
 * Build hierarchical tree structure from flat branch list
 * @param {Array} branches Flat list of branches
 * @returns {Array} Tree structure
 */
function buildTree(branches) {
    const map = new Map();
    const roots = [];

    // First pass: create map of all nodes
    branches.forEach(branch => {
        map.set(branch.uuid, { ...branch, children: [] });
    });

    // Second pass: build parent-child relationships
    branches.forEach(branch => {
        const node = map.get(branch.uuid);
        if (branch.parent_uuid && map.has(branch.parent_uuid)) {
            map.get(branch.parent_uuid).children.push(node);
        } else {
            // No parent or parent not found = root node
            roots.push(node);
        }
    });

    return roots;
}

/**
 * Delete a single branch and update indices
 * @param {string} uuid UUID of branch to delete
 * @param {Object} branch Branch object
 */
async function deleteBranch(uuid, branch) {
    // Remove from branch storage
    await storage.removeItem(`branch:${uuid}`);

    // Remove from character index
    if (branch.character_id) {
        const charBranches = await storage.getItem(`char:${branch.character_id}`) || [];
        const filtered = charBranches.filter(id => id !== uuid);
        await storage.setItem(`char:${branch.character_id}`, filtered);
    }

    // Remove from root index
    const rootBranches = await storage.getItem(`root:${branch.root_uuid}`) || [];
    const filtered = rootBranches.filter(id => id !== uuid);
    await storage.setItem(`root:${branch.root_uuid}`, filtered);
}

/**
 * Recursively delete a branch and all its children
 * @param {string} uuid UUID of branch to delete
 * @param {Object} branch Branch object
 */
async function deleteRecursive(uuid, branch) {
    // Get all branches in this root to find children
    const rootBranches = await storage.getItem(`root:${branch.root_uuid}`) || [];
    
    // Find children
    const children = [];
    for (const childUuid of rootBranches) {
        const childBranch = await storage.getItem(`branch:${childUuid}`);
        if (childBranch && childBranch.parent_uuid === uuid) {
            children.push({ uuid: childUuid, branch: childBranch });
        }
    }

    // Recursively delete children
    for (const child of children) {
        await deleteRecursive(child.uuid, child.branch);
    }

    // Delete this branch
    await deleteBranch(uuid, branch);
}

/**
 * Delete all branch data for a character
 * @param {string} characterId Character ID to delete
 * @returns {Promise<number>} Number of branches deleted
 */
async function deleteCharacterData(characterId) {
    // Get all branch UUIDs for this character
    let branchUuids = await storage.getItem(`char:${characterId}`) || [];

    // Deduplicate UUIDs
    branchUuids = [...new Set(branchUuids)];

    if (branchUuids.length === 0) {
        console.log('[Chat Branches] No branches found for character:', characterId);
        return 0;
    }

    console.log(`[Chat Branches] Deleting ${branchUuids.length} branches for character:`, characterId);

    // Delete each branch and its data
    let deletedCount = 0;
    for (const uuid of branchUuids) {
        const branch = await storage.getItem(`branch:${uuid}`);
        if (branch) {
            await deleteBranch(uuid, branch);
            deletedCount++;
        }
    }

    // Remove the character index itself
    await storage.removeItem(`char:${characterId}`);

    return deletedCount;
}

/**
 * Clean up on server shutdown
 * @returns {Promise<void>}
 */
async function exit() {
    console.log('[Chat Branches] Shutting down plugin...');
    if (initialized) {
        // node-persist handles cleanup automatically
        initialized = false;
    }
}

module.exports = {
    init,
    exit,
    info: {
        id: 'chat-branches-plugin',
        name: 'Chat Branches Tracker',
        description: 'High-performance branch relationship tracking for SillyTavern'
    }
};