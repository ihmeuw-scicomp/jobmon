import React from 'react'
import { Modal, Button } from 'react-bootstrap';


export default function CustomModal({headerContent, bodyContent, showModal, setShowModal, className=''}) {



    return (
        <Modal
            size="lg"
            aria-labelledby="contained-modal-title-vcenter"
            centered
            show={showModal}
            onHide={() => { setShowModal(false) }}
            className={className}
        >
            <Modal.Header>
                <Modal.Title id="contained-modal-title-vcenter">
                    {headerContent}
                </Modal.Title>
            </Modal.Header>
            <Modal.Body>
                <pre>
                    {bodyContent}
                </pre>
            </Modal.Body>
            <Modal.Footer>
                <Button variant="primary" onClick={() => { setShowModal(false) }}>
                    Close
                </Button>
            </Modal.Footer>
        </Modal>
  )
}
