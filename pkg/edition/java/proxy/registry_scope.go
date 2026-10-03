package proxy

import "go.minekube.com/gate/pkg/util/uuid"

type scopedPlayerID struct {
	scope string
	id    uuid.UUID
}

type scopedPlayerName struct {
	scope string
	name  string
}

// PlayerRegistryScope returns the immutable scope captured before authentication.
// The default scope is empty. Wrappers around Player can preserve the scope by
// implementing RegistryScope() string; this does not extend the Player interface.
func PlayerRegistryScope(player Player) string {
	if scoped, ok := player.(interface{ RegistryScope() string }); ok {
		return scoped.RegistryScope()
	}
	return ""
}

func (p *connectedPlayer) RegistryScope() string { return p.registryScope }

// PlayersInScope returns a snapshot of online players in exactly this scope.
func (p *Proxy) PlayersInScope(scope string) []Player {
	p.muP.RLock()
	defer p.muP.RUnlock()
	var result []Player
	for key, player := range p.playerIDs {
		if key.scope == scope {
			result = append(result, player)
		}
	}
	return result
}

// PlayerInScope returns the online player with this UUID in exactly this scope.
func (p *Proxy) PlayerInScope(scope string, id uuid.UUID) Player {
	p.muP.RLock()
	defer p.muP.RUnlock()
	if player := p.playerIDs[scopedPlayerID{scope, id}]; player != nil {
		return player
	}
	return nil
}

// PlayerByNameInScope looks up a case-insensitive username in exactly this scope.
func (p *Proxy) PlayerByNameInScope(scope, username string) Player {
	if player := p.playerByNameInScope(scope, username); player != nil {
		return player
	}
	return nil
}
