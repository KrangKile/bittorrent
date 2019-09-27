#!/usr/bin/python

import random

from math import floor
from operator import itemgetter
from collections import Counter
from messages import Upload
from krankilestd import KrankileStd


class KrankilePropshare(KrankileStd):
    # This client uses the same requesting strategy as the standard client,
    # so we chose to jsut let it inherit the request method from that.

    def uploads(self, requests, peers, history):
        """
        requests -- a list of the requests for this peer for this round
        peers -- available info about all the peers
        history -- history for all previous rounds

        returns: list of Upload objects.

        In each round, this will be called after requests().
        """

        # If there are no requests, just terminate
        if len(requests) == 0:
            return []

        # Initialize a counter for keeping track of how much people let us download last round
        uploader_c = Counter()
        last = history.downloads[-1]

        # Get a set of all peers who requested pieces from us
        requester_ids = set(x.requester_id for x in requests)

        for download in last:
            if download.from_id in requester_ids:
                uploader_c.update({download.from_id: download.blocks})

        total = sum(uploader_c.values())

        id_and_bw = []
        # Variable used to add excess bandwidth from flooring
        not_used_float = 0
        # Calculate bandwidth distribution according strategy described in the pset
        for peer_id in uploader_c:
            bw = floor((uploader_c[peer_id]/total)*0.9*self.up_bw)
            not_used_float += (uploader_c[peer_id]/total)*0.9*self.up_bw - bw
            id_and_bw.append([peer_id, bw])

        not_used_int = floor(not_used_float)

        chosen = set(x[0] for x in uploader_c)
        peer_ids = set(x.requester_id for x in requests).difference(chosen)

        # If there is peers to choose from, add one to optimistically unchoke
        if bool(peer_ids):
            id_and_bw.append(
                [random.choice(list(peer_ids)), floor(self.up_bw*0.1)])

        # If there is bw not used -> distribute it evenly across the peers we are unchoking
        id_and_bw = sorted(id_and_bw, key=itemgetter(1), reverse=True)
        index = 0
        while not_used_int > 0:
            id_and_bw[index % len(id_and_bw)][1] += 1
            index += 1
            not_used_int -= 1

        # Create uploads out of the list of peer ids and bandwidths
        uploads = [Upload(self.id, peer_id, bw) for (peer_id, bw) in id_and_bw]

        return uploads
